import unittest
from livestore import Position
from .common import capture, full, part, playlist, resources, rows, requests, Stage, Reply


class Failures(unittest.TestCase):
    def test_fallback(self):
        delta = playlist(10, full("/b"), skip=1, ended=True)
        complete = playlist(10, full("/a") + full("/b"), ended=True)
        with capture([Stage(delta, resources(a=b"A", b=b"B"), complete)]) as (source, collector, archive):
            collector.poll()
            self.assertTrue(collector.settle()["complete"])
            self.assertEqual([r["query"] for r in requests(source, "/live.m3u8")],
                             ["_HLS_skip=YES", ""])
            self.assertEqual(rows(archive), [((0, 10), b"A"), ((0, 11), b"B")])

    def test_parts_survive_failure_and_expiry(self):
        stages = [
            Stage(playlist(39, full("/old") + part("/p0") + part("/p1")),
                  {"/old": (Reply(b"old"),), "/p0": (Reply(b"A"),),
                   "/p1": (Reply(b"B", gate="p1"),)}),
            Stage(playlist(39, full("/broken", 2), skip=1),
                  {"/broken": (Reply(b"unavailable", 503),)}),
            Stage(playlist(41, ended=True)),
        ]
        with capture(stages) as (source, collector, archive):
            collector.poll()
            collector.wait_for_download(Position(39, 0))
            collector.wait_for_download(Position(40, 0), 0)
            source.wait_requests("/p1")
            self.assertEqual(rows(archive), [((0, 39), b"old")])
            source.advance(1)
            collector.poll()
            source.wait_responses("/broken")
            source.release("p1")
            status = collector.settle()
            self.assertEqual(rows(archive), [((0, 39), b"old"), ((0, 40), b"AB")])
            self.assertTrue(any("503" in e["error"] for e in status["download_errors"]))
            source.advance(2)
            collector.poll()
            self.assertTrue(collector.settle()["complete"])
            self.assertEqual(len(requests(source, "/p0")), 1)
            self.assertEqual(len(requests(source, "/p1")), 1)

    def test_unsealed_tail_survives_slide(self):
        stages = [
            Stage(playlist(9, full("/old") + part("/p0") + part("/p1")),
                  {"/old": (Reply(b"old"),), "/p0": (Reply(b"A"),), "/p1": (Reply(b"", 503),)}),
            Stage(playlist(11, full("/new"), ended=True), resources(new=b"new")),
        ]
        with capture(stages) as (source, collector, archive):
            collector.poll()
            collector.settle()
            source.advance(1)
            collector.poll()
            status = collector.settle()
            self.assertFalse(status["complete"])
            self.assertEqual(rows(archive), [((0, 9), b"old"), ((0, 11), b"new")])
            self.assertEqual([p.position.identity for p in archive.read().pending], [(0, 10)])
            self.assertIn("410", archive.read().pending[0].reason)

    def test_retry(self):
        stage = Stage(playlist(2, full("/a"), ended=True),
                      {"/a": (Reply(b"retry", 503), Reply(b"ok"))})
        with capture([stage]) as (_, collector, archive):
            collector.poll()
            first = collector.settle()
            self.assertFalse(first["complete"])
            self.assertIn("503", first["pending"][0]["reason"])
            collector.poll()
            self.assertTrue(collector.settle()["complete"])
            self.assertEqual(rows(archive), [((0, 2), b"ok")])

    def test_no_fallback_context_remains_pending(self):
        bad = playlist(5, full("/x"), skip=2, ended=True)
        with capture([Stage(bad, resources(x=b"X"))]) as (source, collector, archive):
            status = collector.poll()
            self.assertTrue(status["need_full"])
            self.assertFalse(status["complete"])
            self.assertIsNotNone(status["playlist_error"])
            self.assertEqual(rows(archive), [])
            self.assertFalse(requests(source, "/x"))

    def test_bad_playlist_keeps_published(self):
        stages = [Stage(playlist(1, full("/ok")), resources(ok=b"ok")),
                  Stage("not a playlist")]
        with capture(stages) as (source, collector, archive):
            collector.poll()
            collector.settle()
            source.advance(1)
            status = collector.poll()
            self.assertIsNotNone(status["playlist_error"])
            self.assertFalse(status["complete"])
            self.assertEqual(rows(archive), [((0, 1), b"ok")])
