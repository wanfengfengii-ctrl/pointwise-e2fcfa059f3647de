import unittest
from livestore import Session, PlaylistError
from .common import capture, full, part, playlist, resources, rows, Stage


class Delta(unittest.TestCase):
    def test_continuation(self):
        stages = [
            Stage(playlist(30, full("/a") + full("/b")), resources(a=b"A", b=b"B")),
            Stage(playlist(30, full("/c"), skip=2, ended=True), resources(c=b"C")),
        ]
        with capture(stages) as (source, collector, archive):
            collector.poll()
            collector.settle()
            source.advance(1)
            collector.poll()
            self.assertTrue(collector.settle()["complete"])
            self.assertEqual(rows(archive), [((0, 30), b"A"), ((0, 31), b"B"), ((0, 32), b"C")])

    def test_unknown_skip_is_transactional(self):
        session = Session()
        session.accept_playlist(playlist(4, full("/four")))
        before = session.declarations
        update = session.accept_playlist(playlist(4, full("/six"), skip=2))
        self.assertTrue(update.need_full)
        self.assertEqual(session.declarations, before)
        self.assertEqual(update.declarations, ())

    def test_skip_cannot_consume_unclosed_tail(self):
        session = Session()
        session.accept_playlist(playlist(4, part("/p")))
        self.assertTrue(session.accept_playlist(playlist(4, full("/five"), skip=1)).need_full)

    def test_skipped_discontinuity(self):
        session = Session()
        session.accept_playlist(playlist(8, full("/a") + "#EXT-X-DISCONTINUITY\n" + full("/b")))
        update = session.accept_playlist(playlist(8, full("/c"), skip=2))
        self.assertFalse(update.need_full)
        self.assertEqual([d.position.identity for d in update.declarations], [(0, 8), (1, 9), (1, 10)])
        # A window may remove the leading discontinuity tag and increase its header.
        update = session.accept_playlist(playlist(9, full("/d"), disc=1, skip=2))
        self.assertFalse(update.need_full)
        self.assertEqual(update.declarations[-1].position.identity, (1, 11))

    def test_bad_update_preserves_session(self):
        session = Session()
        session.accept_playlist(playlist(1, part("/p") + full("/one", 2)))
        before = session.declarations
        with self.assertRaises(PlaylistError):
            session.accept_playlist(playlist(1, part("/p", 3) + full("/one", 2)))
        self.assertEqual(session.declarations, before)

    def test_gap_is_reported_without_invention(self):
        stages = [Stage(playlist(20, full("/a")), resources(a=b"A")),
                  Stage(playlist(22, full("/c"), ended=True), resources(c=b"C"))]
        with capture(stages) as (source, collector, archive):
            collector.poll()
            collector.settle()
            source.advance(1)
            collector.poll()
            status = collector.settle()
            self.assertEqual(status["missing_ranges"], [[21, 22]])
            self.assertFalse(status["complete"])
            self.assertEqual(rows(archive), [((0, 20), b"A"), ((0, 22), b"C")])

    def test_rejects_out_of_scope_and_malformed(self):
        for body in ('#EXT-X-KEY:METHOD=AES-128,URI="x"\n',
                     '#EXT-X-MAP:URI="init"\n',
                     '#EXT-X-BYTERANGE:4@0\n',
                     '#EXT-X-STREAM-INF:BANDWIDTH=100\nx\n',
                     '#EXT-X-PART:DURATION=NaN,URI="x"\n',
                     '#EXTINF:1,\n',
                     '#EXT-X-PART:DURATION=1,URI="x",URI="y"\n'):
            with self.subTest(body=body), self.assertRaises(PlaylistError):
                Session().accept_playlist(playlist(0, body))

    def test_fallback_window_cannot_hide_missing_context(self):
        session = Session()
        self.assertTrue(session.accept_playlist(playlist(40, full("/41"), skip=1)).need_full)
        session.accept_playlist(playlist(42, full("/42"), ended=True))
        self.assertEqual(session.missing_ranges(), ((40, 42),))

    def test_omitted_part_tags_preserve_known_prefix(self):
        session = Session()
        session.accept_playlist(playlist(40, part("/p0") + part("/p1")))
        declaration = session.accept_playlist(playlist(40, full("/full", 2))).declarations[0]
        self.assertEqual([p.uri for p in declaration.parts], ["/p0", "/p1"])
        self.assertEqual(declaration.sealed_parts, 2)
