import hashlib
from pathlib import Path
import tempfile
import unittest
from livestore import Archive, Position
from .common import capture, full, playlist, resources, rows, Stage


class Normal(unittest.TestCase):
    def test_full(self):
        stage = Stage(playlist(7, full("/a") + full("/b"), ended=True),
                      resources(a=b"\x00A", b=b"B\xff"))
        with capture([stage]) as (_, collector, archive):
            collector.poll()
            self.assertTrue(collector.settle()["complete"])
            self.assertEqual(rows(archive), [((0, 7), b"\x00A"), ((0, 8), b"B\xff")])
            self.assertFalse(archive.read().pending)
            self.assertEqual([s.sha256 for s in archive.read().segments],
                             [hashlib.sha256(x).hexdigest() for x in (b"\x00A", b"B\xff")])

    def test_repeat(self):
        stage = Stage(playlist(7, full("/a") + full("/b"), ended=True),
                      resources(a=b"A", b=b"B"))
        with capture([stage]) as (source, collector, archive):
            for _ in range(4):
                collector.poll()
                collector.settle()
            self.assertEqual(rows(archive), [((0, 7), b"A"), ((0, 8), b"B")])
            media = [r for r in source.trace if r["event"] == "request" and r["path"] != "/live.m3u8"]
            self.assertEqual(len(media), 2)

    def test_disk_reads(self):
        with tempfile.TemporaryDirectory(prefix="livestore-test-") as directory:
            path = str(Path(directory) / "archive.sqlite")
            stage = Stage(playlist(1, full("/x"), ended=True), resources(x=b"persisted"))
            with capture([stage], path) as (_, collector, archive):
                collector.poll()
                collector.settle()
                with Archive(path) as reader:
                    self.assertEqual(rows(reader), [((0, 1), b"persisted")])
            with Archive(path) as reopened:
                self.assertEqual(rows(reopened), [((0, 1), b"persisted")])

    def test_archive_conflict_preserves_old(self):
        from livestore.playlist import Declaration
        with Archive() as archive:
            p = Position(5, 2)
            archive.declare([Declaration(p)])
            self.assertTrue(archive.publish(p, b"good"))
            self.assertFalse(archive.publish(p, b"good"))
            with self.assertRaises(ValueError):
                archive.publish(p, b"bad")
            self.assertEqual(rows(archive), [((2, 5), b"good")])

    def test_empty_ended(self):
        with capture([Stage(playlist(0, ended=True))]) as (_, collector, archive):
            collector.poll()
            self.assertTrue(collector.settle()["complete"])
            self.assertEqual(rows(archive), [])
