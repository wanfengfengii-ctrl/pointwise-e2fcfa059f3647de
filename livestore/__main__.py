"""Real capture, ordered archive inspection, and finite recording demonstration."""
import argparse
import json
import sqlite3
from pathlib import Path
import tempfile
from . import Archive, Collector, Position
from .source import Recording


def snapshot_json(archive):
    result = archive.read()
    return {
        "segments": [{"position": list(s.position.identity), "body_hex": s.body.hex(),
                      "sha256": s.sha256} for s in result.segments],
        "pending": [{"position": list(p.position.identity), "reason": p.reason}
                    for p in result.pending],
    }


def demo():
    recording = Path(__file__).resolve().parent.parent / "samples" / "recording.json"
    with tempfile.TemporaryDirectory(prefix="livestore-demo-") as directory:
        path = str(Path(directory) / "archive.sqlite")
        with Archive(path) as archive:
            source = Recording.from_json(recording)
            collector = Collector(source.url, archive)
            try:
                collector.poll()
                collector.wait_for_download(Position(40, 0), 0)
                source.wait_requests("/p1")
                source.wait_requests("/reuse")
                source.advance(1)
                collector.poll()
                collector.wait_for_download(Position(41, 0))
                source.release("part")
                collector.wait_for_download(Position(40, 0), 1)
                source.advance(2)
                collector.poll()
                collector.wait_for_download(Position(42, 1))
                source.wait_responses("/reuse", 1)
                source.release("late")
                status = collector.settle()
                source.wait_responses("/reuse", 2)
                with Archive(path) as reader:
                    actual = [(s.position.identity, s.body) for s in reader.read().segments]
                    expected = [((0, 40), b"A40B40"), ((0, 41), b"S41"), ((1, 42), b"S42")]
                    if actual != expected or not status["complete"]:
                        raise ValueError("recorded capture differs from original media")
                    print(json.dumps({"archive": snapshot_json(reader), "capture": status,
                                      "trace": source.trace}, indent=2))
            finally:
                try:
                    source.close()
                finally:
                    collector.close()
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("demo")
    read = commands.add_parser("read")
    read.add_argument("--db", required=True)
    capture = commands.add_parser("capture")
    capture.add_argument("url")
    capture.add_argument("--db", required=True)
    capture.add_argument("--polls", type=int, default=3)
    args = parser.parse_args()
    try:
        if args.command == "demo":
            return demo()
        with Archive(args.db) as archive:
            if args.command == "read":
                print(json.dumps(snapshot_json(archive), indent=2))
                return 0
            if args.polls < 1:
                raise ValueError("polls must be positive")
            with Collector(args.url, archive) as collector:
                for _ in range(args.polls):
                    collector.poll()
                    collector.settle()
                status = collector.status()
                print(json.dumps({"archive": snapshot_json(archive), "capture": status}, indent=2))
                return 0 if status["complete"] else 1
    except (OSError, ValueError, TimeoutError, sqlite3.Error) as exc:
        print(json.dumps({"error": type(exc).__name__ + ": " + str(exc)}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
