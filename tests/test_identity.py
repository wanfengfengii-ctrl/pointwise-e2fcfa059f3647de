import unittest
from livestore import Position
from .common import capture, full, playlist, rows, requests, Stage, Reply


class Identity(unittest.TestCase):
    def test_late_across_discontinuity(self):
        stages = [
            Stage(playlist(70, full("/same")), {"/same": (Reply(b"OLD", gate="old"),)}),
            Stage(playlist(71, full("/same"), disc=1, ended=True), {"/same": (Reply(b"NEW"),)}),
        ]
        with capture(stages) as (source, collector, archive):
            collector.poll()
            source.wait_requests("/same")
            source.advance(1)
            collector.poll()
            collector.wait_for_download(Position(71, 1))
            self.assertEqual(rows(archive), [((1, 71), b"NEW")])
            source.release("old")
            self.assertTrue(collector.settle()["complete"])
            self.assertEqual(rows(archive), [((0, 70), b"OLD"), ((1, 71), b"NEW")])
            self.assertEqual(len(requests(source, "/same")), 2)

    def test_same_uri_in_one_playlist_is_two_positions(self):
        # Body equality does not make positions equivalent.
        stage = Stage(playlist(1, full("/same") + full("/same"), ended=True),
                      {"/same": (Reply(b"identical"),)})
        with capture([stage]) as (source, collector, archive):
            collector.poll()
            self.assertTrue(collector.settle()["complete"])
            self.assertEqual(rows(archive), [((0, 1), b"identical"), ((0, 2), b"identical")])
            self.assertEqual(len(requests(source, "/same")), 2)
