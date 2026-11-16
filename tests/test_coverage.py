import hashlib
import unittest
from livestore import Position, Session
from .common import capture, full, part, playlist, resources, rows, requests, Stage, Reply


class Coverage(unittest.TestCase):
    def test_joint_slide_and_late_full(self):
        stages = [
            Stage(playlist(40, part("/p0") + part("/p1") + full("/reuse", 2)),
                  {"/p0": (Reply(b"A40"),), "/p1": (Reply(b"B40", gate="part"),),
                   "/reuse": (Reply(b"A40B40", gate="late"),)}),
            Stage(playlist(40, full("/s41"), skip=1), resources(s41=b"S41")),
            Stage(playlist(42, "#EXT-X-DISCONTINUITY\n" + full("/reuse"), ended=True),
                  {"/reuse": (Reply(b"S42"),)}),
        ]
        with capture(stages) as (source, collector, archive):
            collector.poll()
            collector.wait_for_download(Position(40, 0), 0)
            source.wait_requests("/p1")
            source.wait_requests("/reuse")
            self.assertEqual(rows(archive), [])
            source.advance(1)
            collector.poll()
            collector.wait_for_download(Position(41, 0))
            source.release("part")
            collector.wait_for_download(Position(40, 0), 1)
            self.assertEqual(rows(archive), [((0, 40), b"A40B40"), ((0, 41), b"S41")])
            source.advance(2)
            collector.poll()
            collector.wait_for_download(Position(42, 1))
            before = rows(archive)
            source.wait_responses("/reuse", 1)
            source.release("late")
            self.assertTrue(collector.settle()["complete"])
            expected = [((0, 40), b"A40B40"), ((0, 41), b"S41"), ((1, 42), b"S42")]
            self.assertEqual(before, expected)
            self.assertEqual(rows(archive), expected)
            self.assertEqual([s.sha256 for s in archive.read().segments],
                             [hashlib.sha256(b).hexdigest() for _, b in expected])
            self.assertEqual(len(requests(source, "/reuse")), 2)
            self.assertEqual(len(requests(source, "/p0")), 1)
            self.assertEqual(len(requests(source, "/p1")), 1)
            source.wait_responses("/reuse", 2)
            responses = [r for r in source.trace if r["event"] == "response" and r["path"] == "/reuse"]
            self.assertEqual([r["stage"] for r in responses], [2, 0])

    def test_tail_endlist_is_not_proof(self):
        with capture([Stage(playlist(1, part("/p"), ended=True), resources(p=b"only"))]) as (_, collector, archive):
            collector.poll()
            self.assertFalse(collector.settle()["complete"])
            self.assertEqual(rows(archive), [])
            self.assertIn("unproven", archive.read().pending[0].reason)

    def test_partial_part_prefix_not_sealed(self):
        session = Session()
        update = session.accept_playlist(playlist(1, part("/p0") + full("/s", 2)))
        self.assertIsNone(update.declarations[0].sealed_parts)
        update = session.accept_playlist(playlist(1, part("/p0") + part("/p1") + full("/s", 2)))
        self.assertEqual(update.declarations[0].sealed_parts, 2)

    def test_full_can_finish_while_parts_fail(self):
        stage = Stage(playlist(1, part("/p") + full("/s")),
                      {"/p": (Reply(b"no", 410),), "/s": (Reply(b"yes"),)})
        with capture([stage]) as (_, collector, archive):
            collector.poll()
            collector.settle()
            self.assertEqual(rows(archive), [((0, 1), b"yes")])
            self.assertFalse(archive.read().pending)
