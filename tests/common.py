from contextlib import contextmanager
from livestore import Archive, Collector
from livestore.source import Recording, Stage, Reply


def playlist(base, content="", disc=0, ended=False, skip=None):
    header = "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:%s\n#EXT-X-DISCONTINUITY-SEQUENCE:%s\n" % (base, disc)
    if skip is not None:
        header += "#EXT-X-SKIP:SKIPPED-SEGMENTS=%s\n" % skip
    return header + content + ("#EXT-X-ENDLIST\n" if ended else "")


def full(uri, duration=1):
    return "#EXTINF:%s,\n%s\n" % (duration, uri)


def part(uri, duration=1):
    return '#EXT-X-PART:DURATION=%s,URI="%s"\n' % (duration, uri)


def resources(**items):
    return {"/" + name: (Reply(body),) for name, body in items.items()}


@contextmanager
def capture(stages, db=":memory:"):
    archive = Archive(db)
    source = Recording(stages)
    collector = Collector(source.url, archive)
    try:
        yield source, collector, archive
    finally:
        try:
            source.close()
        finally:
            try:
                collector.close()
            finally:
                archive.close()


def rows(archive):
    return [(s.position.identity, s.body) for s in archive.read().segments]


def requests(source, path):
    return [r for r in source.trace if r["event"] == "request" and r["path"] == path]
