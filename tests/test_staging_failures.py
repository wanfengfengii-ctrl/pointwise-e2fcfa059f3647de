"""Failure regression for the disk-backed payload staging path.

Injection points (all explicit, documented test seams):

* Staging.write_fault(path, chunk_index) is called inside the streaming
  store() loop immediately before a chunk hits disk; raising simulates a
  write failure. store() removes the partial file itself, so no half
  representation becomes visible.
* Staging.read_fault(path) is called every time a stored payload is
  re-opened for publication or alternative verification; raising simulates
  a disk read failure before any byte leaves the file.
* Archive.commit_fault(position) is called inside the publish transaction
  after the blob was streamed but before commit; raising forces the
  surrounding context manager to roll the whole insert back.

Every case checks that previously published bytes survive, that no
half-written segment is readable through Archive.read(), that the staged
candidate remains retryable afterwards and that close() reclaims every
file this session created without touching the caller database.
"""
import os
from pathlib import Path
import tempfile
import unittest
from livestore import Archive, Collector, Position
from livestore.source import Recording
from livestore.staging import Staging
from .common import full, part, playlist, resources, rows, Stage, Reply


class StagingUnit(unittest.TestCase):
    def setUp(self):
        self.staging = Staging()

    def tearDown(self):
        self.staging.close()

    def _files(self):
        return sorted(p.name for p in self.staging.root.iterdir())

    def test_write_failure_discards_partial_file(self):
        position = Position(3, 0)

        def fault(path, index):
            if index == 2:
                raise OSError("simulated write failure")

        self.staging.write_fault = fault
        chunks = [b"aa", b"bb", b"cc", b"dd"]
        with self.assertRaises(OSError):
            self.staging.store(position, None, iter(chunks), expected_length=8)
        self.assertEqual(self._files(), [])

        # The same binding remains retryable and succeeds once healthy.
        self.staging.write_fault = None
        ticket = self.staging.store(position, None, iter(chunks), expected_length=8)
        self.assertEqual(ticket.size, 8)
        self.assertEqual(self._files(), [ticket.path.name])
        with self.staging.open_read(ticket) as handle:
            self.assertEqual(handle.read(), b"aabbccdd")

    def test_short_transfer_is_a_failure_not_a_short_segment(self):
        position = Position(4, 0)
        with self.assertRaises(OSError):
            self.staging.store(position, None, iter([b"ab", b"cd"]), expected_length=8)
        self.assertEqual(self._files(), [])

    def test_close_removes_the_whole_session_directory(self):
        root = self.staging.root
        ticket = self.staging.store(Position(1, 0), None, iter([b"x"]))
        self.assertTrue(ticket.path.exists())
        self.staging.close()
        self.assertFalse(root.exists())
        # Idempotent close must not raise during exception cleanup.
        self.staging.close()


class StagingThroughCollector(unittest.TestCase):
    def test_download_write_failure_is_retryable_without_half_segment(self):
        stage = Stage(playlist(1, full("/a") + full("/b"), ended=True),
                      resources(a=b"alpha", b=b"bravo"))
        source = Recording([stage])
        archive = Archive()
        collector = Collector(source.url, archive)
        try:
            def fault(path, index):
                # Fail the /a download (sequence 1, full representation)
                # deterministically on its first chunk; /b stays healthy.
                if "p1-d0-full" in path.name and index == 0:
                    raise OSError("simulated staging write failure")

            collector._staging.write_fault = fault
            collector.poll()
            status = collector.settle()
            # The failed representation never produced a visible segment...
            self.assertEqual(rows(archive), [((0, 2), b"bravo")])
            # ...its half file was reclaimed and the error diagnosed...
            self.assertEqual(os.listdir(collector._staging.root), [])
            self.assertTrue(any(e["position"] == [0, 1] for e in status["download_errors"]))

            # ...and clearing the fault lets a later poll retry the same
            # binding to a complete publication.
            collector._staging.write_fault = None
            collector.poll()
            self.assertTrue(collector.settle()["complete"])
            self.assertEqual(rows(archive), [((0, 1), b"alpha"), ((0, 2), b"bravo")])
            self.assertEqual(os.listdir(collector._staging.root), [])
        finally:
            source.close()
            collector.close()
            archive.close()

    def test_reread_failure_keeps_candidate_and_old_publication(self):
        # Stage 0 declares the parts without an EXTINF, so the part set is
        # not proven complete and the staged payloads cannot publish yet.
        unsealed = Stage(playlist(1, part("/p0") + part("/p1")),
                         {"/p0": (Reply(b"AB"),), "/p1": (Reply(b"CD"),)})
        sealed = Stage(playlist(1, part("/p0") + part("/p1") + full("/s", 2), ended=True),
                       {"/p0": (Reply(b"AB"),), "/p1": (Reply(b"CD"),), "/s": (Reply(b"ABCD"),)})
        source = Recording([unsealed, sealed])
        archive = Archive()
        collector = Collector(source.url, archive)
        try:
            collector.poll()
            collector.wait_for_download(Position(1, 0), 0, timeout=30)
            collector.wait_for_download(Position(1, 0), 1, timeout=30)
            self.assertEqual(rows(archive), [])
            # A later list proves the part set complete; arm a read fault so
            # publication cannot stream the staged payloads into SQLite.
            source.advance(1)
            collector._staging.read_fault = lambda path: (_ for _ in ()).throw(
                OSError("simulated read failure"))
            with self.assertRaises(OSError):
                collector.poll()
            # No half segment is visible and the staged candidates survive.
            self.assertEqual(rows(archive), [])
            self.assertGreaterEqual(len(os.listdir(collector._staging.root)), 2)

            # Once the disk is healthy again the very same staged tickets
            # publish, proving the work stayed retryable.
            collector._staging.read_fault = None
            self.assertTrue(collector.poll()["complete"])
            self.assertEqual(rows(archive), [((0, 1), b"ABCD")])
            self.assertEqual(os.listdir(collector._staging.root), [])
        finally:
            source.close()
            collector.close()
            archive.close()

    def test_commit_failure_rolls_back_and_preserves_prior_segments(self):
        with tempfile.TemporaryDirectory(prefix="livestore-fault-") as directory:
            db = str(Path(directory) / "archive.sqlite")
            first = Stage(playlist(1, full("/a") + full("/b"), ended=True),
                          resources(a=b"first", b=b"second"))
            source = Recording([first])
            archive = Archive(db)
            collector = Collector(source.url, archive)
            try:
                collector.poll()
                collector.settle()
                self.assertEqual(
                    rows(archive), [((0, 1), b"first"), ((0, 2), b"second")])
            finally:
                source.close()
                collector.close()
                archive.close()

            # A second session over a new source must not expose a half
            # publication when the commit fails.
            second = Stage(playlist(3, full("/c"), ended=True), resources(c=b"third"))
            source2 = Recording([second])
            archive2 = Archive(db)
            collector2 = Collector(source2.url, archive2)
            try:
                attempts = {"n": 0}

                def fault(position):
                    attempts["n"] += 1
                    raise OSError("simulated commit failure")

                archive2.commit_fault = fault
                collector2.poll()
                with self.assertRaises(OSError):
                    collector2.settle()
                self.assertGreaterEqual(attempts["n"], 1)
                # Rolled back: no half row, previous publications intact.
                self.assertEqual(
                    rows(archive2), [((0, 1), b"first"), ((0, 2), b"second")])
                # The staged candidate is still present for a retry.
                self.assertEqual(len(os.listdir(collector2._staging.root)), 1)
                archive2.commit_fault = None
                collector2.poll()
                self.assertTrue(collector2.settle()["complete"])
                self.assertEqual(
                    rows(archive2),
                    [((0, 1), b"first"), ((0, 2), b"second"), ((0, 3), b"third")])
                self.assertEqual(os.listdir(collector2._staging.root), [])
            finally:
                source2.close()
                collector2.close()
                archive2.close()

    def test_close_reclaims_accepted_but_unpublished_staging_and_keeps_database(self):
        with tempfile.TemporaryDirectory(prefix="livestore-fault-") as directory:
            db = str(Path(directory) / "archive.sqlite")
            stage = Stage(playlist(1, part("/p0") + part("/p1"), ended=True),
                          {"/p0": (Reply(b"AB"),), "/p1": (Reply(b"CD"),)})
            source = Recording([stage])
            archive = Archive(db)
            collector = Collector(source.url, archive)
            try:
                collector.poll()
                collector.wait_for_download(Position(1, 0), 0, timeout=30)
                collector.wait_for_download(Position(1, 0), 1, timeout=30)
                # Both parts landed on disk but the part set is never proven
                # complete (no EXTINF match), so nothing was published.
                stage_root = collector._staging.root
                self.assertEqual(len(os.listdir(stage_root)), 2)
                self.assertEqual(rows(archive), [])
            finally:
                # close() reclaims accepted payloads that never published;
                # the caller's database file must remain in place.
                collector.close()
                source.close()
                archive.close()
            self.assertFalse(stage_root.exists())
            self.assertTrue(Path(db).exists())


if __name__ == "__main__":
    unittest.main()
