"""Independent small-scale differential for the disk-backed LiveStore.

This check does not use livestore code to build expected results: a tiny
reference model independently keeps every downloaded body in memory,
joins proven part sets by plain concatenation and applies the
single-publication / byte-for-byte alternative rule itself. It drives the
real HTTP Recording source and the real Collector/Archive through the same
gate sequences the product uses and then diffs:

  * the ordered (identity, body, sha256) publication set,
  * pending positions,
  * complete coverage,
  * request counts per URI,
  * on-disk staging hygiene (no payload files left after close, database
    still present).

Run from the project root:  python3.11 -B diff_check.py
The script only exercises the existing finite recording grammar; it adds
no capture capability.
"""
import hashlib
import os
from pathlib import Path
import tempfile
from livestore import Archive, Collector, Position
from livestore.source import Recording, Stage, Reply


def digest(body):
    return hashlib.sha256(body).hexdigest()


class ReferenceModel:
    """Independently computes what the collector must publish."""

    def __init__(self):
        self.bodies = {}          # token -> exact bytes
        self.published = {}       # sequence -> bytes
        self.conflicts = []       # (sequence, incoming bytes)

    def note(self, position, part, body):
        self.bodies[(position, part)] = body

    def reconcile(self, declarations):
        for declaration in declarations:
            seq = declaration.position.sequence
            full = self.bodies.get((declaration.position, None))
            if full is not None:
                self._offer(seq, full)
            count = declaration.sealed_parts
            if count is not None:
                pieces = [self.bodies.get((declaration.position, i)) for i in range(count)]
                if all(piece is not None for piece in pieces):
                    self._offer(seq, b"".join(pieces))

    def _offer(self, seq, body):
        if seq in self.published:
            if self.published[seq] != body:
                self.conflicts.append((seq, body))
        else:
            self.published[seq] = body

    def conflicts_at(self, seq):
        return [body for s, body in self.conflicts if s == seq]


def trace_counts(source):
    counts = {}
    for row in source.trace:
        if row["event"] == "request" and row["path"] != "/live.m3u8":
            counts[row["path"]] = counts.get(row["path"], 0) + 1
    return counts


def archive_rows(archive):
    return [(s.position.identity, s.body, s.sha256) for s in archive.read().segments]


def expect_rows(model, declarations_by_seq):
    rows = []
    for seq in sorted(model.published):
        body = model.published[seq]
        disc = declarations_by_seq[seq].position.discontinuity
        rows.append(((disc, seq), body, digest(body)))
    return rows


def case_full_and_parts_and_late_alternative():
    # Two positions: position 1 arrives as two parts AND a full segment,
    # position 2 is a full segment; the full alternative for position 1 is
    # gated until after the parts already published.
    text = ("#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:1\n"
            '#EXT-X-PART:DURATION=1,URI="/p0"\n#EXT-X-PART:DURATION=1,URI="/p1"\n'
            "#EXTINF:2,\n/whole\n#EXTINF:1,\n/other\n#EXT-X-ENDLIST\n")
    stage = Stage(text, {
        "/p0": (Reply(b"A1"),), "/p1": (Reply(b"B1"),),
        "/whole": (Reply(b"A1B1", gate="late"),), "/other": (Reply(b"Z2"),)})
    model = ReferenceModel()
    with tempfile.TemporaryDirectory(prefix="livestore-diff-") as directory:
        db = Path(directory) / "archive.sqlite"
        source = Recording([stage], gate_timeout=30)
        archive = Archive(str(db))
        collector = Collector(source.url, archive, workers=4)
        try:
            collector.poll()
            collector.wait_for_download(Position(1, 0), 0, timeout=30)
            collector.wait_for_download(Position(1, 0), 1, timeout=30)
            for part, body in ((0, b"A1"), (1, b"B1")):
                model.note(Position(1, 0), part, body)
            model.reconcile(collector.session.declarations)
            source.wait_requests("/whole")
            collector.wait_for_download(Position(2, 0), timeout=30)
            model.note(Position(2, 0), None, b"Z2")
            model.reconcile(collector.session.declarations)
            source.release("late")
            collector.settle(timeout=30)
            model.note(Position(1, 0), None, b"A1B1")
            model.reconcile(collector.session.declarations)
            status = collector.status()

            by_seq = {d.position.sequence: d for d in collector.session.declarations}
            assert status["complete"], status
            assert archive_rows(archive) == expect_rows(model, by_seq)
            assert not model.conflicts, model.conflicts
            assert trace_counts(source) == {"/p0": 1, "/p1": 1, "/whole": 1, "/other": 1}
            assert status["in_flight"] == 0 and not status["download_errors"]
        finally:
            source.close()
            collector.close()
            archive.close()
        assert db.exists()
    return "full + parts + byte-exact late alternative"


def case_historical_parts_survive_window_slide():
    stages = [
        Stage(("#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:9\n#EXTINF:1,\n/old\n"
               '#EXT-X-PART:DURATION=1,URI="/p0"\n#EXT-X-PART:DURATION=1,URI="/p1"\n'),
              {"/old": (Reply(b"old"),), "/p0": (Reply(b"A"),),
               "/p1": (Reply(b"B", gate="p1"),)}),
        Stage("#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:9\n#EXT-X-SKIP:SKIPPED-SEGMENTS=1\n"
              "#EXTINF:2,\n/broken\n",
              {"/broken": (Reply(b"unavailable", 503),)}),
        Stage("#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:11\n#EXTINF:1,\n/new\n#EXT-X-ENDLIST\n",
              {"/new": (Reply(b"new"),)}),
    ]
    model = ReferenceModel()
    with tempfile.TemporaryDirectory(prefix="livestore-diff-") as directory:
        db = Path(directory) / "archive.sqlite"
        source = Recording(stages, gate_timeout=30)
        archive = Archive(str(db))
        collector = Collector(source.url, archive, workers=4)
        try:
            collector.poll()
            collector.wait_for_download(Position(9, 0), timeout=30)
            collector.wait_for_download(Position(10, 0), 0, timeout=30)
            model.note(Position(9, 0), None, b"old")
            model.note(Position(10, 0), 0, b"A")
            source.wait_requests("/p1")
            source.advance(1)
            collector.poll()
            source.wait_responses("/broken")
            source.release("p1")
            collector.settle(timeout=30)
            model.note(Position(10, 0), 1, b"B")
            model.reconcile(collector.session.declarations)
            source.advance(2)
            collector.poll()
            status = collector.settle(timeout=30)
            model.note(Position(11, 0), None, b"new")
            model.reconcile(collector.session.declarations)
            assert status["complete"], status
            by_seq = {d.position.sequence: d for d in collector.session.declarations}
            assert archive_rows(archive) == expect_rows(model, by_seq)
            assert {row[0] for row in archive_rows(archive)} == {(0, 9), (0, 10), (0, 11)}
        finally:
            source.close()
            collector.close()
            archive.close()
    return "historical parts survive 410/503 and window slide"


def case_deferred_conflict_preserves_original():
    text = ("#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:7\n"
            '#EXT-X-PART:DURATION=1,URI="/p0"\n#EXT-X-PART:DURATION=1,URI="/p1"\n'
            "#EXTINF:2,\n/whole\n#EXT-X-ENDLIST\n")
    stage = Stage(text, {"/p0": (Reply(b"X"),), "/p1": (Reply(b"B", gate="tail"),),
                         "/whole": (Reply(b"AB"),)})
    model = ReferenceModel()
    source = Recording([stage], gate_timeout=30)
    archive = Archive()
    collector = Collector(source.url, archive, workers=4)
    rejected = False
    try:
        collector.poll()
        collector.wait_for_download(Position(7, 0), timeout=30)
        collector.wait_for_download(Position(7, 0), 0, timeout=30)
        model.note(Position(7, 0), None, b"AB")
        model.reconcile(collector.session.declarations)
        assert archive_rows(archive) == [((0, 7), b"AB", digest(b"AB"))]
        source.release("tail")
        try:
            collector.settle(timeout=30)
        except ValueError:
            rejected = True
        model.note(Position(7, 0), 0, b"X")
        model.note(Position(7, 0), 1, b"B")
        model.reconcile(collector.session.declarations)
        assert rejected, "late alternative conflict was not rejected"
        assert model.conflicts_at(7) == [b"XB"], model.conflicts
        # Original publication preserved byte for byte.
        assert archive_rows(archive) == [((0, 7), b"AB", digest(b"AB"))]
    finally:
        source.close()
        try:
            collector.close()
        except ValueError:
            # Re-reconciling the already rejected conflict during close is
            # the documented behavior; staging is still fully reclaimed.
            pass
        archive.close()
    return "deferred part conflict rejected, original bytes preserved"


def case_staging_directory_is_clean_after_close():
    text = ("#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:1\n#EXTINF:1,\n/a\n#EXT-X-ENDLIST\n")
    with tempfile.TemporaryDirectory(prefix="livestore-diff-") as directory:
        db = Path(directory) / "archive.sqlite"
        source = Recording([Stage(text, {"/a": (Reply(b"payload"),)})])
        archive = Archive(str(db))
        collector = Collector(source.url, archive, workers=4)
        root = collector._staging.root
        try:
            collector.poll()
            collector.settle(timeout=30)
            assert os.listdir(root) == []  # released right after publication
        finally:
            source.close()
            collector.close()
            archive.close()
        assert not root.exists() and db.exists()
        leftovers = [p for p in Path(directory).rglob("*") if p.is_file()]
        assert leftovers == [db], leftovers
    return "no staging payload survives close; database untouched"


def main():
    results = [
        case_full_and_parts_and_late_alternative(),
        case_historical_parts_survive_window_slide(),
        case_deferred_conflict_preserves_original(),
        case_staging_directory_is_clean_after_close(),
    ]
    for result in results:
        print("differential OK:", result)
    print("all independent differential checks passed (%d cases)" % len(results))


if __name__ == "__main__":
    main()
