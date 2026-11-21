"""SQLite access for the lake temperature archive.

Design invariant
----------------
The restored temperature (``restored_mc``) of every frame is persisted and is
authoritative.  A frame's *stored* representation is derived from it:

    absolute frame : stored = restored
    relative frame : stored = restored - restored(base_seq)

Consequently, correcting a base frame

  * changes the restored temperature of that frame only;
  * leaves every other frame's restored temperature and base reference intact;
  * only changes the derived stored difference of frames that reference it.

A correction reads and writes just the addressed frame (plus, for a relative
frame, its base's restored value).

Stale-page protection
---------------------
A correction carries ``seen_restored_mc``: the temperature the page displayed.
If the archive's current restored value differs, the submission is rejected
with 409 and nothing is written.  A submission whose restored value is
unchanged is accepted even when the stored representation has drifted (e.g.
its base frame was corrected in the meantime).
"""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator

from . import csvio

_SCHEMA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema.sql")

# Frames are inserted in batches of this size.  Each batch is its own short
# ``BEGIN IMMEDIATE`` transaction, so between batches the database write lock
# is free and corrections to other archives proceed normally while a large
# import is still running.  The archive row stays ``importing`` -- and thus
# invisible -- until the very last batch has been committed.
IMPORT_BATCH_SIZE = 200

# An ``importing`` archive older than this is presumed abandoned by a killed
# process and is swept at start-up.  Young ones are left alone so that a
# second process starting against the same database never deletes a live
# import (a large upload takes seconds to minutes, not hours).
STALE_IMPORT_AFTER_HOURS = 1


class ArchiveError(Exception):
    """Domain error carrying an HTTP status code for the UI."""

    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.message = message
        self.status = status


class ConflictError(ArchiveError):
    def __init__(self, message: str):
        super().__init__(message, status=409)


@dataclass(frozen=True)
class FrameView:
    seq: int
    sampled_at: str
    sampled_at_raw: str
    restored_mc: int
    base_seq: int | None
    stored_kind: str
    stored_mc: int
    kind_raw: str = ""
    extras: tuple[str, ...] = ()


@dataclass(frozen=True)
class ArchiveSummary:
    id: int
    name: str
    source_filename: str
    frame_count: int
    created_at: str


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=10.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 10000")
    return conn


def init_db(db_path: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        # Migrate first: schema.sql creates the index on archive.status, which
        # a pre-migration database lacks the column for.
        _migrate(conn)
        with open(_SCHEMA_PATH, "r", encoding="utf-8") as fh:
            conn.executescript(fh.read())
        _sweep_stale_imports(conn)
    finally:
        conn.close()


def _sweep_stale_imports(conn: sqlite3.Connection) -> None:
    """Remove archives left half-written by a previously crashed process.

    ``create_archive`` cleans up after any exception, so only a killed process
    can leave an ``importing`` row behind.  The age guard matters when more
    than one process serves the same database: a process starting up must not
    delete an import another process is actively running.  A row is therefore
    only swept once it has been importing for longer than
    ``STALE_IMPORT_AFTER_HOURS``; it is invisible in the meantime, so leaving
    a young one alone costs nothing.
    """
    with _transaction(conn):
        conn.execute(
            "DELETE FROM archive WHERE status = 'importing'"
            " AND julianday('now') - julianday(created_at) > ?",
            (STALE_IMPORT_AFTER_HOURS / 24.0,),
        )


def _migrate(conn: sqlite3.Connection) -> None:
    """Bring a database created by an older schema up to date.

    The only change to the schema so far is ``archive.status``; databases
    made before it hold only fully imported archives, so the column is added
    with the ``ready`` default.  On a fresh database there is nothing to do --
    schema.sql creates the table with the column already present.
    """
    if conn.execute("SELECT 1 FROM sqlite_master"
                    " WHERE type = 'table' AND name = 'archive'").fetchone() is None:
        return
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(archive)")}
    if "status" not in columns:
        with _transaction(conn):
            conn.execute(
                "ALTER TABLE archive ADD COLUMN status TEXT NOT NULL"
                " DEFAULT 'ready' CHECK (status IN ('importing', 'ready'))"
            )


@contextmanager
def _transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def _stored(restored_mc: int, base_restored_mc: int | None) -> tuple[str, int]:
    if base_restored_mc is None:
        return "absolute", restored_mc
    return "delta", restored_mc - base_restored_mc


def _view(row: sqlite3.Row, base_restored_mc: int | None) -> FrameView:
    kind, stored = _stored(row["restored_mc"], base_restored_mc)
    return FrameView(
        seq=row["seq"],
        sampled_at=row["sampled_at"],
        sampled_at_raw=row["sampled_at_raw"],
        restored_mc=row["restored_mc"],
        base_seq=row["base_seq"],
        stored_kind=kind,
        stored_mc=stored,
        kind_raw=row["kind_raw"],
        extras=tuple(json.loads(row["extras_json"])) if row["extras_json"] else (),
    )


def _fetch_base_restored(conn, archive_id: int, base_seq: int | None) -> int | None:
    if base_seq is None:
        return None
    row = conn.execute(
        "SELECT restored_mc FROM frame WHERE archive_id = ? AND seq = ?",
        (archive_id, base_seq),
    ).fetchone()
    if row is None:
        raise ArchiveError("base frame %d is missing from the archive" % base_seq, status=500)
    return row["restored_mc"]


def create_archive(db_path: str, name: str, filename: str, payload: bytes) -> int:
    """Import a logger CSV as a new archive; returns the archive id.

    The archive is row-visible as ``importing`` from the first committed batch
    on, but every public read filters it out until the closing transaction
    sets ``ready``.  Committing batch by batch keeps the SQLite write lock
    free between batches, so corrections to *other* archives are not held up
    by a large import.

    If any step fails, this import's own rows are deleted (a correction to
    some other archive is a separate committed transaction and is untouched),
    then the error is re-raised.  Deleting the archive row frees its unique
    name for an immediate retry.
    """
    rows, layout = csvio.parse_csv(payload)
    restored: dict[int, int] = {}
    for fr in rows:  # ordered by seq, and base_seq < seq
        if fr.stored_kind == "absolute":
            restored[fr.seq] = fr.stored_mc
        else:
            restored[fr.seq] = fr.stored_mc + restored[fr.base_seq]

    conn = connect(db_path)
    archive_id: int | None = None
    try:
        with _transaction(conn):
            try:
                cur = conn.execute(
                    "INSERT INTO archive (name, source_filename, layout_json, status)"
                    " VALUES (?, ?, ?, 'importing')",
                    (name, filename, layout.to_json()),
                )
            except sqlite3.IntegrityError as exc:
                if "archive.name" in str(exc):
                    raise ConflictError(
                        "an archive named %r already exists" % name) from exc
                raise
            archive_id = cur.lastrowid

        for start in range(0, len(rows), IMPORT_BATCH_SIZE):
            batch = rows[start:start + IMPORT_BATCH_SIZE]
            with _transaction(conn):
                conn.executemany(
                    "INSERT INTO frame (archive_id, seq, sampled_at, sampled_at_raw,"
                    " restored_mc, base_seq, kind_raw, extras_json)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [(archive_id, fr.seq, fr.sampled_at, fr.sampled_at_raw,
                      restored[fr.seq], fr.base_seq, fr.kind_raw,
                      json.dumps(list(fr.extras), ensure_ascii=False)) for fr in batch],
                )

        # The closing batch is the moment the archive becomes usable: listing,
        # frame lookup, correction and download all see it in this one commit.
        with _transaction(conn):
            conn.execute(
                "UPDATE archive SET status = 'ready' WHERE id = ?", (archive_id,)
            )
        return archive_id
    except BaseException:
        _discard_import(conn, archive_id)
        raise
    finally:
        conn.close()


def _discard_import(conn: sqlite3.Connection, archive_id: int | None) -> None:
    """Delete a partially imported archive and everything already written.

    Runs as its own transaction; ``ON DELETE CASCADE`` removes its frame
    rows.  Failures here must not mask the original import error, so they are
    swallowed -- a leftover ``importing`` archive is never visible through the
    UI and keeps its name reserved only until cleanup succeeds.
    """
    if archive_id is None:
        return
    try:
        # A failed COMMIT can leave the transaction open, in which case a new
        # BEGIN would itself fail; roll it back before cleaning up.
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        with _transaction(conn):
            conn.execute("DELETE FROM archive WHERE id = ?", (archive_id,))
    except sqlite3.Error:
        pass


def list_archives(db_path: str) -> list[ArchiveSummary]:
    conn = connect(db_path)
    try:
        rows = conn.execute(
            "SELECT a.id, a.name, a.source_filename, a.created_at, COUNT(f.seq) AS n"
            " FROM archive a LEFT JOIN frame f ON f.archive_id = a.id"
            " WHERE a.status = 'ready'"
            " GROUP BY a.id ORDER BY a.id"
        ).fetchall()
        return [
            ArchiveSummary(id=r["id"], name=r["name"],
                           source_filename=r["source_filename"],
                           frame_count=r["n"], created_at=r["created_at"])
            for r in rows
        ]
    finally:
        conn.close()


def _archive_row(conn, archive_id: int) -> sqlite3.Row:
    """Fetch a *ready* archive; one still importing is reported as absent."""
    row = conn.execute(
        "SELECT * FROM archive WHERE id = ? AND status = 'ready'", (archive_id,)
    ).fetchone()
    if row is None:
        raise ArchiveError("archive not found", status=404)
    return row


def get_archive(db_path: str, archive_id: int) -> ArchiveSummary:
    conn = connect(db_path)
    try:
        row = _archive_row(conn, archive_id)
        n = conn.execute(
            "SELECT COUNT(*) FROM frame WHERE archive_id = ?", (archive_id,)
        ).fetchone()[0]
        return ArchiveSummary(id=row["id"], name=row["name"],
                              source_filename=row["source_filename"],
                              frame_count=n, created_at=row["created_at"])
    finally:
        conn.close()


def get_frame(dbp: str, archive_id: int, seq: int) -> FrameView:
    """Read just the addressed frame (and its base's restored value).

    Only rows of this archive are read.
    """
    conn = connect(dbp)
    try:
        _archive_row(conn, archive_id)
        row = conn.execute(
            "SELECT * FROM frame WHERE archive_id = ? AND seq = ?", (archive_id, seq)
        ).fetchone()
        if row is None:
            raise ArchiveError("frame %d not found in this archive" % seq, status=404)
        return _view(row, _fetch_base_restored(conn, archive_id, row["base_seq"]))
    finally:
        conn.close()


def get_frame_bundle(dbp: str, archive_id: int, seq: int):
    """Read the addressed frame, its base, and the frames that cite it as base.

    Used only to render the single-frame page.  The correction path itself
    reads and writes nothing but the addressed frame (and the base value it is
    relative to); this listing merely warns the operator which stored
    differences will move when a base frame is corrected.
    """
    conn = connect(dbp)
    try:
        _archive_row(conn, archive_id)
        row = conn.execute(
            "SELECT * FROM frame WHERE archive_id = ? AND seq = ?", (archive_id, seq)
        ).fetchone()
        if row is None:
            raise ArchiveError("frame %d not found in this archive" % seq, status=404)
        base_val = _fetch_base_restored(conn, archive_id, row["base_seq"])
        base = None
        if row["base_seq"] is not None:
            base_row = conn.execute(
                "SELECT * FROM frame WHERE archive_id = ? AND seq = ?",
                (archive_id, row["base_seq"]),
            ).fetchone()
            base = _view(base_row, _fetch_base_restored(conn, archive_id, base_row["base_seq"]))

        ref_rows = conn.execute(
            "SELECT * FROM frame WHERE archive_id = ? AND base_seq = ? ORDER BY seq",
            (archive_id, seq),
        ).fetchall()
        referrers = [_view(r, row["restored_mc"]) for r in ref_rows]
        return _view(row, base_val), base, referrers
    finally:
        conn.close()


def list_frames(db_path: str, archive_id: int) -> list[FrameView]:
    conn = connect(db_path)
    try:
        _archive_row(conn, archive_id)
        rows = conn.execute(
            "SELECT * FROM frame WHERE archive_id = ? ORDER BY seq", (archive_id,)
        ).fetchall()
        by_seq = {r["seq"]: r["restored_mc"] for r in rows}
        return [
            _view(r, by_seq.get(r["base_seq"]) if r["base_seq"] is not None else None)
            for r in rows
        ]
    finally:
        conn.close()


def correct_frame(db_path: str, archive_id: int, seq: int, *,
                  seen_restored_mc: int, new_restored_mc: int) -> FrameView:
    """Apply a correction to one frame; return the updated view.

    Raises :class:`ConflictError` when the temperature the submitter saw no
    longer matches the stored restored value.  Nothing is written in that case.
    """
    if new_restored_mc < -1_000_000 or new_restored_mc > 1_000_000:
        raise ArchiveError("restored temperature is outside the supported range")

    conn = connect(db_path)
    try:
        with _transaction(conn):
            _archive_row(conn, archive_id)
            row = conn.execute(
                "SELECT * FROM frame WHERE archive_id = ? AND seq = ?",
                (archive_id, seq),
            ).fetchone()
            if row is None:
                raise ArchiveError("frame %d not found in this archive" % seq, status=404)

            old_restored = row["restored_mc"]
            old_base = row["base_seq"]
            old_base_restored = _fetch_base_restored(conn, archive_id, old_base)
            old_kind, old_stored = _stored(old_restored, old_base_restored)

            if seen_restored_mc != old_restored:
                raise ConflictError(
                    "页面显示的温度是 %d m°C，但该帧当前的还原值是 %d m°C；"
                    "更正已拒绝，现档保持不变。" % (seen_restored_mc, old_restored)
                )

            new_kind, new_stored = _stored(new_restored_mc, old_base_restored)

            if new_restored_mc != old_restored:
                conn.execute(
                    "UPDATE frame SET restored_mc = ? WHERE archive_id = ? AND seq = ?",
                    (new_restored_mc, archive_id, seq),
                )
                conn.execute(
                    "INSERT INTO revision (archive_id, seq, old_restored_mc, old_base_seq,"
                    " old_stored_kind, old_stored_mc, new_restored_mc, new_base_seq,"
                    " new_stored_kind, new_stored_mc, seen_restored_mc)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (archive_id, seq, old_restored, old_base, old_kind, old_stored,
                     new_restored_mc, old_base, new_kind, new_stored, seen_restored_mc),
                )

            return FrameView(
                seq=row["seq"], sampled_at=row["sampled_at"],
                sampled_at_raw=row["sampled_at_raw"],
                restored_mc=new_restored_mc, base_seq=old_base,
                stored_kind=new_kind, stored_mc=new_stored,
                kind_raw=row["kind_raw"],
                extras=tuple(json.loads(row["extras_json"])),
            )
    finally:
        conn.close()


def export_csv(db_path: str, archive_id: int) -> bytes:
    """Render every frame of one archive from a single consistent snapshot.

    The whole read runs inside one transaction, so the file reflects one
    committed state; a correction cannot be half-visible in a download.
    """
    conn = connect(db_path)
    try:
        conn.execute("BEGIN")
        try:
            row = _archive_row(conn, archive_id)
            layout = csvio.CsvLayout.from_json(row["layout_json"])
            rows = conn.execute(
                "SELECT * FROM frame WHERE archive_id = ? ORDER BY seq", (archive_id,)
            ).fetchall()
            by_seq = {r["seq"]: r["restored_mc"] for r in rows}
            frames = [
                _view(r, by_seq.get(r["base_seq"]) if r["base_seq"] is not None else None)
                for r in rows
            ]
        finally:
            conn.execute("COMMIT")
    finally:
        conn.close()
    return csvio.render_csv(frames, layout)
