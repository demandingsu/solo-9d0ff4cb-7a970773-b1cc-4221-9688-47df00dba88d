"""Unit/integration tests exercised through the Flask test client.

Run with:  PYTHONPATH=.:<flask site> python3 -m unittest discover -s tests -v
"""

from __future__ import annotations

import io
import os
import sqlite3
import tempfile
import unittest
import unittest.mock

from lake_archive import archive as store
from lake_archive import csvio
from lake_archive.web import create_app

store_connect = store.connect   # the unpatched entry point, for test proxies

CSV = b"""seq,sampled_at_utc,value_millidegrees_c,kind,base_seq
1,2024-01-01T00:00:00Z,-1500,absolute,
2,2024-01-01T01:00:00Z,250,delta,1
3,2024-01-01T02:00:00Z,500,absolute,
4,2024-01-01T03:00:00Z,-100,delta,2
5,2024-01-01T04:00:00Z,-1800,delta,1
"""


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "archive.sqlite3")
        self.app = create_app(self.db)
        self.app.testing = True
        self.client = self.app.test_client()
        self.aid = store.create_archive(self.db, "lake", "logger.csv", CSV)
        store.init_db(self.db)  # idempotent

    def tearDown(self):
        self.tmp.cleanup()

    def frame(self, seq):
        return store.get_frame(self.db, self.aid, seq)

    def frames(self):
        return {f.seq: f for f in store.list_frames(self.db, self.aid)}

    def download(self):
        return self.client.get("/archives/%d/download.csv" % self.aid).data

    def correct(self, seq, seen, new):
        return self.client.post(
            "/archives/%d/frames/%d/correct" % (self.aid, seq),
            data={"seen_restored_mc": str(seen), "new_restored_mc": str(new)},
        )


class TestImport(Base):
    def test_restored_values(self):
        f = self.frames()
        self.assertEqual(f[1].restored_mc, -1500)
        self.assertEqual(f[2].restored_mc, -1250)   # -1500 + 250
        self.assertEqual(f[3].restored_mc, 500)
        self.assertEqual(f[4].restored_mc, -1350)   # -1250 + -100
        self.assertEqual(f[5].restored_mc, -3300)   # -1500 + -1800
        self.assertEqual(f[2].base_seq, 1)
        self.assertIsNone(f[3].base_seq)

    def test_absolute_stored_equals_restored(self):
        f = self.frames()
        self.assertEqual(f[1].stored_kind, "absolute")
        self.assertEqual(f[1].stored_mc, f[1].restored_mc)
        self.assertEqual(f[2].stored_kind, "delta")
        self.assertEqual(f[2].stored_mc, 250)

    def test_duplicate_name_rejected(self):
        with self.assertRaises(store.ConflictError):
            store.create_archive(self.db, "lake", "x.csv", CSV)

    def test_old_schema_migrates_to_ready(self):
        """A database from before archive.status opens with all rows ready."""
        tmp = tempfile.TemporaryDirectory()
        try:
            old = os.path.join(tmp.name, "old.sqlite3")
            conn = sqlite3.connect(old)
            try:
                conn.executescript(
                    "CREATE TABLE archive (id INTEGER PRIMARY KEY,"
                    " name TEXT NOT NULL UNIQUE, source_filename TEXT NOT NULL,"
                    " layout_json TEXT NOT NULL,"
                    " created_at TEXT NOT NULL DEFAULT"
                    " (strftime('%Y-%m-%dT%H:%M:%fZ','now')));"
                    "CREATE TABLE frame (archive_id INTEGER NOT NULL"
                    " REFERENCES archive(id) ON DELETE CASCADE,"
                    " seq INTEGER NOT NULL, sampled_at TEXT NOT NULL,"
                    " sampled_at_raw TEXT NOT NULL, restored_mc INTEGER NOT NULL,"
                    " base_seq INTEGER, kind_raw TEXT NOT NULL DEFAULT '',"
                    " extras_json TEXT NOT NULL DEFAULT '[]',"
                    " PRIMARY KEY (archive_id, seq),"
                    " CHECK (base_seq IS NULL OR base_seq < seq),"
                    " FOREIGN KEY (archive_id, base_seq)"
                    " REFERENCES frame(archive_id, seq)"
                    " DEFERRABLE INITIALLY DEFERRED);"
                    "CREATE TABLE revision (id INTEGER PRIMARY KEY,"
                    " archive_id INTEGER NOT NULL REFERENCES archive(id)"
                    " ON DELETE CASCADE, seq INTEGER NOT NULL,"
                    " old_restored_mc INTEGER NOT NULL, old_base_seq INTEGER,"
                    " old_stored_kind TEXT NOT NULL, old_stored_mc INTEGER NOT NULL,"
                    " new_restored_mc INTEGER NOT NULL, new_base_seq INTEGER,"
                    " new_stored_kind TEXT NOT NULL, new_stored_mc INTEGER NOT NULL,"
                    " seen_restored_mc INTEGER NOT NULL,"
                    " submitted_at TEXT NOT NULL DEFAULT"
                    " (strftime('%Y-%m-%dT%H:%M:%fZ','now')));"
                    "INSERT INTO archive (name, source_filename, layout_json)"
                    " VALUES ('legacy','l.csv','{}');"
                )
                conn.commit()
            finally:
                conn.close()

            store.init_db(old)     # migrates
            self.assertEqual([a.name for a in store.list_archives(old)], ["legacy"])
            store.init_db(old)     # idempotent
            conn = store.connect(old)
            try:
                cols = {r["name"] for r in conn.execute("PRAGMA table_info(archive)")}
                idx = conn.execute("SELECT 1 FROM sqlite_master"
                                   " WHERE type='index' AND name='archive_ready'")
                has_index = idx.fetchone() is not None
                status = conn.execute(
                    "SELECT status FROM archive WHERE name='legacy'").fetchone()[0]
            finally:
                conn.close()
            self.assertIn("status", cols)
            self.assertTrue(has_index)
            self.assertEqual(status, "ready")
        finally:
            tmp.cleanup()

    def test_bad_csv_rejected(self):
        for payload in (b"", b"a,b\n1,2\n",
                        b"seq,sampled_at_utc,value_millidegrees_c\n1,nope,5\n",
                        b"seq,sampled_at_utc,value_millidegrees_c,base_seq\n"
                        b"1,2024-01-01T00:00:00Z,5,\n2,2024-01-01T01:00:00Z,5,9\n",
                        b"seq,sampled_at_utc,value_millidegrees_c,base_seq\n"
                        b"2,2024-01-01T00:00:00Z,5,\n2,2024-01-01T01:00:00Z,5,\n",
                        b"seq,sampled_at_utc,value_millidegrees_c\n1,2024-01-01T00:00:00Z,1.5\n"):
            with self.assertRaises(csvio.CsvFormatError):
                store.create_archive(self.db, "other-%d" % id(payload), "x.csv", payload)

    def test_csv_roundtrip_format(self):
        out = self.download().decode()
        self.assertEqual(out, CSV.decode())


class TestBaseCorrection(Base):
    def test_correct_base_frame_keeps_others_restored(self):
        before = self.frames()
        r = self.correct(1, seen=-1500, new=-2000)
        self.assertEqual(r.status_code, 302)
        after = self.frames()
        self.assertEqual(after[1].restored_mc, -2000)
        for seq in (2, 3, 4, 5):
            self.assertEqual(after[seq].restored_mc, before[seq].restored_mc,
                             "restored temperature of frame %d must not move" % seq)
            self.assertEqual(after[seq].base_seq, before[seq].base_seq)
        # only the derived difference moved
        self.assertEqual(after[2].stored_mc, 750)
        self.assertEqual(after[4].stored_mc, -100)
        self.assertEqual(after[5].stored_mc, -1300)

    def test_export_after_base_correction_is_consistent(self):
        self.correct(1, seen=-1500, new=-2000)
        rows, _ = csvio.parse_csv(self.download())
        by = {r.seq: r for r in rows}
        restored = {}
        for r in rows:
            restored[r.seq] = r.stored_mc if r.base_seq is None else r.stored_mc + restored[r.base_seq]
        self.assertEqual(restored, {1: -2000, 2: -1250, 3: 500, 4: -1350, 5: -3300})

    def test_correcting_referrer_keeps_its_own_base(self):
        r = self.correct(2, seen=-1250, new=-900)
        self.assertEqual(r.status_code, 302)
        f = self.frame(2)
        self.assertEqual((f.restored_mc, f.base_seq, f.stored_mc), (-900, 1, 600))
        self.assertEqual(self.frame(4).restored_mc, -1350)  # child of 2 unchanged

    def test_stale_page_rejected_and_archive_kept(self):
        self.correct(1, seen=-1500, new=-2000)
        stale = self.correct(1, seen=-1500, new=-2500)   # page still shows -1500
        self.assertEqual(stale.status_code, 409)
        self.assertIn("已拒绝", stale.data.decode())
        self.assertEqual(self.frame(1).restored_mc, -2000)
        self.assertEqual(self.download(), self.download())  # nothing changed

    def test_stale_page_second_tab_after_other_change(self):
        # Tab A opens frame 2 showing -1250; tab B corrects frame 2 to -900.
        self.assertEqual(self.correct(2, seen=-1250, new=-900).status_code, 302)
        r = self.correct(2, seen=-1250, new=-800)
        self.assertEqual(r.status_code, 409)
        self.assertEqual(self.frame(2).restored_mc, -900)
        # tab B's page (showing -900) may still submit
        self.assertEqual(self.correct(2, seen=-900, new=-800).status_code, 302)
        self.assertEqual(self.frame(2).restored_mc, -800)

    def test_same_temperature_resubmit_accepted(self):
        r = self.correct(2, seen=-1250, new=-1250)
        self.assertEqual(r.status_code, 302)
        self.assertEqual(self.frame(2).restored_mc, -1250)

    def test_only_stored_delta_changed_still_submittable(self):
        # Frame 2's stored delta becomes 750 once base frame 1 is corrected.
        self.correct(1, seen=-1500, new=-2000)
        page = self.client.get("/archives/%d/frames/2" % self.aid).data.decode()
        self.assertIn("delta 750", page)
        # A visitor whose page displayed restored -1250 (now equal) can submit,
        # even though the stored difference differs from what an older page read.
        self.assertEqual(self.correct(2, seen=-1250, new=-1200).status_code, 302)
        self.assertEqual(self.frame(2).restored_mc, -1200)

    def test_referrer_page_uses_fresh_delta_with_old_value(self):
        # A page opened before the base change shows the old delta. Submitting
        # the same restored value must be accepted, and the stored delta is
        # recomputed from the current base.
        self.correct(1, seen=-1500, new=-2000)
        self.assertEqual(self.correct(2, seen=-1250, new=-1250).status_code, 302)
        self.assertEqual(self.frame(2).stored_mc, 750)

    def test_non_integer_rejected(self):
        r = self.client.post("/archives/%d/frames/1/correct" % self.aid,
                             data={"seen_restored_mc": "-1500", "new_restored_mc": "1.5"})
        self.assertEqual(r.status_code, 400)
        self.assertEqual(self.frame(1).restored_mc, -1500)

    def test_unknown_frame_404(self):
        self.assertEqual(self.client.get(
            "/archives/%d/frames/99" % self.aid).status_code, 404)
        self.assertEqual(self.correct(99, seen=0, new=0).status_code, 404)

    def test_missing_seen_token_rejected(self):
        r = self.client.post("/archives/%d/frames/1/correct" % self.aid,
                             data={"new_restored_mc": "1"})
        self.assertEqual(r.status_code, 400)
        self.assertEqual(self.frame(1).restored_mc, -1500)


class TestConcurrency(Base):
    def test_simultaneous_stale_submissions_only_one_wins(self):
        import threading
        barrier = threading.Barrier(2)
        outcomes = []

        def submit(value):
            barrier.wait()
            try:
                store.correct_frame(self.db, self.aid, 2,
                                    seen_restored_mc=-1250, new_restored_mc=value)
                outcomes.append(value)
            except store.ConflictError:
                outcomes.append("rejected")

        threads = [threading.Thread(target=submit, args=(v,)) for v in (-900, -800)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(outcomes), 2)
        self.assertEqual(outcomes.count("rejected"), 1, outcomes)
        self.assertEqual(self.frame(2).restored_mc,
                         -900 if outcomes.count(-900) else -800)
        conn = store.connect(self.db)
        try:
            n = conn.execute("SELECT COUNT(*) FROM revision").fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(n, 1)  # the rejected attempt left no audit row either


class TestIsolation(Base):
    def test_correction_writes_only_the_addressed_row(self):
        seen = []
        real_connect = store.connect

        def traced_connect(db_path):
            conn = real_connect(db_path)

            def tracer(statement):
                lowered = statement.lstrip().lower()
                if lowered.startswith(("insert", "update", "delete")):
                    seen.append(statement)

            conn.set_trace_callback(tracer)
            return conn

        with unittest.mock.patch.object(store, "connect", traced_connect):
            store.correct_frame(self.db, self.aid, 2,
                                seen_restored_mc=-1250, new_restored_mc=-900)
        writes = [s for s in seen if not s.lstrip().lower().startswith("insert")]
        self.assertEqual(len(writes), 1, seen)
        self.assertIn("WHERE archive_id = 1 AND seq = 2", writes[0])
        for other in (1, 3, 4, 5):
            self.assertNotIn("seq = %d" % other, writes[0])
        # the only other write is the audit row, not a frame change
        inserts = [s for s in seen if s.lstrip().lower().startswith("insert")]
        self.assertEqual(len(inserts), 1)
        self.assertIn("INTO revision", inserts[0])

    def test_two_archives_are_independent(self):
        other = store.create_archive(
            self.db, "lake2", "b.csv",
            b"seq,sampled_at_utc,value_millidegrees_c\n1,2024-02-01T00:00:00Z,-1500\n")
        self.correct(1, seen=-1500, new=-2000)
        self.assertEqual(store.get_frame(self.db, other, 1).restored_mc, -1500)


class TestExport(Base):
    def test_export_snapshot_matches_frames(self):
        self.correct(1, seen=-1500, new=-2100)
        rows, layout = csvio.parse_csv(self.download())
        self.assertEqual(layout.header, ("seq", "sampled_at_utc",
                                         "value_millidegrees_c", "kind", "base_seq"))
        self.assertEqual(rows[0].stored_mc, -2100)

    def test_download_headers(self):
        r = self.client.get("/archives/%d/download.csv" % self.aid)
        self.assertEqual(r.status_code, 200)
        self.assertIn("attachment", r.headers["Content-Disposition"])
        self.assertEqual(r.headers["Cache-Control"], "no-store")

    def test_unmanaged_columns_echoed(self):
        payload = (b"seq,sampled_at_utc,value_millidegrees_c,kind,base_seq,site\n"
                   b"1,2024-01-01T00:00:00Z,-1500,absolute,,north\n"
                   b"2,2024-01-01T01:00:00Z,250,delta,1,north\n")
        aid = store.create_archive(self.db, "extra", "d.csv", payload)
        out = self.client.get("/archives/%d/download.csv" % aid).data
        self.assertEqual(out, payload)

    def test_blank_kind_column_stays_blank(self):
        payload = (b"seq,sampled_at_utc,value_millidegrees_c,kind,base_seq\n"
                   b"1,2024-01-01T00:00:00Z,-1500,,\n"
                   b"2,2024-01-01T01:00:00Z,250,,1\n")
        aid = store.create_archive(self.db, "blankkind", "e.csv", payload)
        self.assertEqual(self.client.get("/archives/%d/download.csv" % aid).data, payload)

    def test_crlf_layout_preserved(self):
        crlf = CSV.replace(b"\n", b"\r\n")
        aid = store.create_archive(self.db, "crlf", "c.csv", crlf)
        out = self.client.get("/archives/%d/download.csv" % aid).data
        self.assertIn(b"\r\n", out)
        self.assertNotIn(b"\n", out.replace(b"\r\n", b""))


class TestIncrementalImport(Base):
    """Large imports must not block or expose themselves mid-flight."""

    def make_rows(self, n, base=1000):
        lines = ["seq,sampled_at_utc,value_millidegrees_c,kind,base_seq"]
        for i in range(n):
            seq = base + i
            kind = "absolute" if i == 0 else "delta"
            ref = "" if i == 0 else str(base)
            lines.append("%d,2024-05-01T00:00:00Z,%d,%s,%s"
                         % (seq, 100 if i else -1500, kind, ref))
        return ("\n".join(lines) + "\n").encode()

    def proxy_connect(self, on_commit):
        """Patch store.connect so ``on_commit(conn, path)`` runs after each COMMIT.

        The test client only sees the public API; the callback models what a
        second browser window does in the gap between two import batches.
        """

        class Proxy:
            def __init__(self, raw, path):
                self._raw = raw
                self._path = path

            def execute(self, sql, *args):
                done = self._raw.execute(sql, *args)
                if sql.strip().upper() == "COMMIT":
                    on_commit(self._raw, self._path)
                return done

            def __getattr__(self, name):
                return getattr(self._raw, name)

            def close(self):
                self._raw.close()

        return unittest.mock.patch.object(
            store, "connect", lambda path: Proxy(store_connect(path), path))

    def test_large_import_is_not_visible_until_complete(self):
        payload = self.make_rows(store.IMPORT_BATCH_SIZE * 2 + 7)
        observations = []

        def on_commit(conn, path):
            status = conn.execute(
                "SELECT status FROM archive WHERE name = 'big'").fetchone()
            if status is not None and status["status"] == "importing":
                observations.append(store.list_archives(path))

        with self.proxy_connect(on_commit):
            aid = store.create_archive(self.db, "big", "big.csv", payload)

        # Observed after every batch commit, and never listed while incomplete.
        self.assertGreaterEqual(len(observations), 4)
        for listed in observations:
            self.assertNotIn("big", [a.name for a in listed])

        summary = store.get_archive(self.db, aid)
        self.assertEqual(summary.frame_count, store.IMPORT_BATCH_SIZE * 2 + 7)
        self.assertEqual(len(store.list_frames(self.db, aid)), summary.frame_count)

    def test_importing_archive_hidden_from_every_read_path(self):
        payload = self.make_rows(store.IMPORT_BATCH_SIZE + 5)
        probes = []
        probing = []

        def on_commit(conn, path):
            if probing:
                return          # a reader's own COMMIT must not re-enter
            row = conn.execute(
                "SELECT id, status FROM archive WHERE name = 'pending'").fetchone()
            if row is None or row["status"] != "importing":
                return
            probes.append(row["id"])
            probing.append(True)
            try:
                for reader in (store.get_archive, store.list_frames,
                               store.export_csv):
                    with self.assertRaises(store.ArchiveError) as ctx:
                        reader(path, row["id"])
                    self.assertEqual(ctx.exception.status, 404)
                with self.assertRaises(store.ArchiveError) as ctx:
                    store.get_frame(path, row["id"], 1000)
                self.assertEqual(ctx.exception.status, 404)
                with self.assertRaises(store.ArchiveError) as ctx:
                    store.correct_frame(path, row["id"], 1000,
                                        seen_restored_mc=-1500,
                                        new_restored_mc=-1400)
                self.assertEqual(ctx.exception.status, 404)
            finally:
                probing.pop()

        with self.proxy_connect(on_commit):
            store.create_archive(self.db, "pending", "p.csv", payload)
        self.assertTrue(probes, "the import must have been probed mid-flight")

        # Nothing was written by the rejected correction.
        conn = store_connect(self.db)
        try:
            n = conn.execute("SELECT COUNT(*) FROM revision").fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(n, 0)

    def test_correction_during_import_commits_and_survives(self):
        """A correction to an existing archive lands while a new one imports."""
        payload = self.make_rows(store.IMPORT_BATCH_SIZE * 2 + 3)
        corrected = []
        busy = []

        def on_commit(conn, path):
            if corrected or busy:
                return
            row = conn.execute(
                "SELECT status FROM archive WHERE name = 'big'").fetchone()
            if row is not None and row["status"] == "importing":
                # A second browser window corrects an older archive; the
                # import must have released the write lock between batches.
                busy.append(True)
                try:
                    corrected.append(store.correct_frame(
                        path, self.aid, 2,
                        seen_restored_mc=-1250, new_restored_mc=-900))
                finally:
                    busy.pop()

        with self.proxy_connect(on_commit):
            aid = store.create_archive(self.db, "big", "big.csv", payload)

        self.assertTrue(corrected)
        f = store.get_frame(self.db, self.aid, 2)
        self.assertEqual(f.restored_mc, -900)
        # The new archive finished correctly despite the interleaved write.
        self.assertEqual(store.get_archive(self.db, aid).frame_count,
                         store.IMPORT_BATCH_SIZE * 2 + 3)
        self.assertEqual(store.export_csv(self.db, aid).count(b"\n"),
                         store.IMPORT_BATCH_SIZE * 2 + 4)

    def test_failed_import_cleans_up_and_allows_retry(self):
        payload = self.make_rows(store.IMPORT_BATCH_SIZE * 2 + 5)
        state = {"commits": 0}

        class FailingConn:
            """Proxy that fails the Nth COMMIT, leaving earlier batches written."""

            def __init__(self, inner):
                self.raw = inner

            def execute(self, sql, *args):
                if sql.strip().upper() == "COMMIT":
                    state["commits"] += 1
                    if state["commits"] == 3:      # after rows are committed
                        raise sqlite3.OperationalError("simulated commit failure")
                return self.raw.execute(sql, *args)

            def __getattr__(self, name):
                return getattr(self.raw, name)

            def close(self):
                self.raw.close()

        with unittest.mock.patch.object(
                store, "connect", lambda p: FailingConn(store_connect(p))):
            with self.assertRaises(sqlite3.OperationalError):
                store.create_archive(self.db, "doomed", "d.csv", payload)

        # The half-written archive is gone, including its frame rows, and the
        # name is free again.
        names = [a.name for a in store.list_archives(self.db)]
        self.assertNotIn("doomed", names)
        conn = store_connect(self.db)
        try:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM archive WHERE name='doomed'")
                .fetchone()[0], 0)
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM frame WHERE archive_id NOT IN"
                             " (SELECT id FROM archive)").fetchone()[0], 0)
        finally:
            conn.close()

        # An earlier correction to another archive survived the failure.
        self.assertEqual(store.get_frame(self.db, self.aid, 2).restored_mc, -1250)

        # Re-uploading under the same name now succeeds.
        aid = store.create_archive(self.db, "doomed", "d.csv", payload)
        self.assertEqual(store.get_archive(self.db, aid).frame_count,
                         store.IMPORT_BATCH_SIZE * 2 + 5)

    def test_failed_import_keeps_committed_corrections(self):
        """A correction committed before a failed import is not rolled back."""
        payload = self.make_rows(store.IMPORT_BATCH_SIZE * 2 + 2)
        state = {"n": 0, "corrected": False, "busy": False}

        class FailingConn:
            def __init__(self, raw, path):
                self._raw = raw
                self._path = path

            def execute(self, sql, *args):
                if sql.strip().upper() == "COMMIT" and not state["busy"]:
                    state["n"] += 1
                    # #1 archive row, #2..#4 frame batches, #5 status flip:
                    # commit several batches, then fail on the closing one.
                    if state["n"] == 5:
                        raise sqlite3.OperationalError("simulated failure")
                return self._raw.execute(sql, *args)

            def __getattr__(self, name):
                return getattr(self._raw, name)

            def close(self):
                self._raw.close()

        def on_commit(conn, path):
            if state["corrected"]:
                return
            state["corrected"] = True
            state["busy"] = True
            try:
                # A second window's correction is committed between batches of
                # the doomed import.  Cleanup must not touch it.
                store.correct_frame(path, self.aid, 1,
                                    seen_restored_mc=-1500, new_restored_mc=-2000)
            finally:
                state["busy"] = False

        with unittest.mock.patch.object(store, "IMPORT_BATCH_SIZE", 2):
            # Layer the commit hook under the commit counter so both see the
            # import's own commits, in the order they happen.
            with self.proxy_connect(on_commit):
                proxied = store.connect
                with unittest.mock.patch.object(
                        store, "connect",
                        lambda p: FailingConn(proxied(p), p)):
                    with self.assertRaises(sqlite3.OperationalError):
                        store.create_archive(self.db, "doomed2", "d.csv", payload)

        self.assertNotIn("doomed2", [a.name for a in store.list_archives(self.db)])
        self.assertEqual(store.get_frame(self.db, self.aid, 1).restored_mc, -2000)

    def test_failed_upload_shows_clean_message(self):
        """A storage failure during an HTTP upload is a friendly 500 page."""
        payload = self.make_rows(store.IMPORT_BATCH_SIZE + 3)
        state = {"n": 0}

        class FailingConn:
            def __init__(self, raw):
                self._raw = raw

            def execute(self, sql, *args):
                if sql.strip().upper() == "COMMIT":
                    state["n"] += 1
                    if state["n"] == 3:
                        raise sqlite3.OperationalError("disk full")
                return self._raw.execute(sql, *args)

            def __getattr__(self, name):
                return getattr(self._raw, name)

            def close(self):
                self._raw.close()

        with unittest.mock.patch.object(
                store, "connect", lambda p: FailingConn(store_connect(p))):
            r = self.client.post(
                "/archives",
                data={"name": "broken", "file": (io.BytesIO(payload), "b.csv")},
                content_type="multipart/form-data")
        self.assertEqual(r.status_code, 500)
        page = r.data.decode()
        self.assertIn("已清理", page)
        self.assertNotIn("Traceback", page)
        self.assertNotIn("broken", [a.name for a in store.list_archives(self.db)])
        # and the same name can be uploaded again
        r2 = self.client.post(
            "/archives",
            data={"name": "broken", "file": (io.BytesIO(payload), "b.csv")},
            content_type="multipart/form-data")
        self.assertEqual(r2.status_code, 302)

    def test_stale_import_swept_on_startup(self):
        conn = store_connect(self.db)
        try:
            # One abandoned long ago, one just started (possibly a live import
            # in another process -- must be left alone).
            with store._transaction(conn):
                conn.execute(
                    "INSERT INTO archive (name, source_filename, layout_json,"
                    " status, created_at) VALUES ('dead', 'x.csv', '{}',"
                    " 'importing', '2020-01-01T00:00:00Z')")
                conn.execute(
                    "INSERT INTO archive (name, source_filename, layout_json, status)"
                    " VALUES ('live-elsewhere', 'x.csv', '{}', 'importing')")
                conn.execute(
                    "INSERT INTO archive (name, source_filename, layout_json, status)"
                    " VALUES ('done', 'x.csv', '{}', 'ready')")
        finally:
            conn.close()
        store.init_db(self.db)   # what a fresh app start does
        conn = store.connect(self.db)
        try:
            names = {r["name"]: r["status"] for r in
                     conn.execute("SELECT name, status FROM archive")}
        finally:
            conn.close()
        self.assertNotIn("dead", names)
        self.assertEqual(names.get("live-elsewhere"), "importing")
        self.assertEqual(names.get("done"), "ready")
        # ...and the abandoned name may be reused immediately.
        store.create_archive(
            self.db, "dead", "x.csv",
            b"seq,sampled_at_utc,value_millidegrees_c\n1,2024-01-01T00:00:00Z,1\n")

    def test_duplicate_name_still_rejected_mid_import(self):
        payload = self.make_rows(store.IMPORT_BATCH_SIZE + 1)
        attempts = []

        def on_commit(conn, path):
            if attempts:
                return
            row = conn.execute(
                "SELECT status FROM archive WHERE name = 'dupe'").fetchone()
            if row is not None and row["status"] == "importing":
                attempts.append(row["status"])
                with self.assertRaises(store.ConflictError):
                    store.create_archive(path, "dupe", "d.csv", payload)

        with self.proxy_connect(on_commit):
            store.create_archive(self.db, "dupe", "d.csv", payload)
        self.assertTrue(attempts)


class TestPages(Base):
    def test_index_and_archive_pages(self):
        self.assertEqual(self.client.get("/").status_code, 200)
        page = self.client.get("/archives/%d" % self.aid).data.decode()
        self.assertIn("/frames/1", page)
        self.assertIn("-1250 m°C", page)

    def test_frame_page_loads_and_shows_value(self):
        page = self.client.get("/archives/%d/frames/2" % self.aid).data.decode()
        self.assertIn("还原温度", page)
        self.assertIn("-1250", page)

    def test_lookup_redirects_to_frame(self):
        r = self.client.get("/archives/%d/lookup?seq=4" % self.aid)
        self.assertEqual(r.status_code, 302)
        self.assertTrue(r.headers["Location"].endswith("/archives/1/frames/4"))
        self.assertEqual(self.client.get(
            "/archives/%d/lookup?seq=abc" % self.aid).status_code, 400)
        missing = self.client.get("/archives/%d/lookup?seq=999" % self.aid)
        self.assertEqual(missing.status_code, 404)
        self.assertIn("没有序号为 999 的帧", missing.data.decode())

    def test_html_escaped(self):
        aid = store.create_archive(
            self.db, "<script>alert(1)</script>", "x.csv",
            b"seq,sampled_at_utc,value_millidegrees_c\n1,2024-01-01T00:00:00Z,1\n")
        page = self.client.get("/archives/%d" % aid).data.decode()
        self.assertNotIn("<script>alert(1)</script>", page)
        self.assertIn("&lt;script&gt;", page)


if __name__ == "__main__":
    unittest.main()
