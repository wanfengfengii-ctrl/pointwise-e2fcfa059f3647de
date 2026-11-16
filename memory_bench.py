"""Public end-to-end memory workloads. No optimized capture implementation."""
import gc
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import tracemalloc
from urllib.parse import urlsplit

BLOCK = 65536
MIB = 1024 * 1024


def block(label):
    return hashlib.sha256(label.encode("ascii")).digest() * (BLOCK // 32)


def chunks(spec):
    for label, length in spec:
        unit = block(label)
        while length:
            take = min(BLOCK, length)
            yield unit[:take]
            length -= take


def playlist(base, text="", ended=False):
    return "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:%d\n%s%s" % (
        base, text, "#EXT-X-ENDLIST\n" if ended else "")


def full(path, duration=1):
    return "#EXTINF:%d,\n%s\n" % (duration, path)


def part(path):
    return '#EXT-X-PART:DURATION=1,URI="%s"\n' % path


def workload(kind, size, conflict=False):
    if kind == "long":
        expected = {i: [(str(i), size // 4)] for i in range(4)}
        resources = {"/l%d" % i: (expected[i], None) for i in range(4)}
        stages = [(playlist(0, "".join(full("/l%d" % i) for i in range(4)), True), resources)]
        return stages, expected
    if kind != "late":
        raise ValueError("unknown workload")
    a = [("a", size // 4)]
    b = [("b", size // 4)]
    c = [("c", size // 4)]
    d = [("d", size // 4)]
    tiny = [("tiny", 17)]
    expected = {40: a + b, 41: c + d, 42: tiny}
    stages = [
        (playlist(40, part("/a") + part("/b")), {"/a": (a, None), "/b": (b, None)}),
        (playlist(41, part("/c") + part("/d") + full("/late", 2) + full("/tiny")),
         {"/c": (c, None), "/d": (d, "part"), "/late": (([("bad", size // 4)] + d) if conflict else c + d, "late"),
          "/tiny": (tiny, None)}),
        (playlist(42, full("/tiny")), {"/tiny": (tiny, None)}),
        (playlist(40, full("/expired", 2) + full("/late", 2) + full("/tiny"), True),
         {"/late": (c + d, None), "/tiny": (tiny, None)}),
    ]
    return stages, expected


def serve(kind, size, conflict):
    stages, _ = workload(kind, size, conflict)
    cv = threading.Condition()
    state = {"stage": 0}
    gates = {"part": threading.Event(), "late": threading.Event()}
    trace = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            path = urlsplit(self.path).path
            with cv:
                at = state["stage"]
                text, resources = stages[at]
                spec, gate = resources.get(path, (None, None))
                trace.append({"path": path, "stage": at})
                cv.notify_all()
            if gate is not None and not gates[gate].wait(90):
                self.send_error(504)
                return
            if path == "/live.m3u8":
                data = text.encode()
                body, length, status = (data,), len(data), 200
            elif spec is None:
                body, length, status = (b"expired",), 7, 410
            else:
                body = chunks(spec)
                length = sum(length for _, length in spec)
                status = 200
            try:
                self.send_response(status)
                self.send_header("Content-Length", str(length))
                self.end_headers()
                for data in body:
                    self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = False
    worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02})
    worker.start()
    print(json.dumps({"url": "http://127.0.0.1:%d/live.m3u8" % server.server_port}), flush=True)
    try:
        for line in sys.stdin:
            request = json.loads(line)
            op = request["op"]
            if op == "advance":
                with cv:
                    assert state["stage"] <= request["stage"] < len(stages)
                    state["stage"] = request["stage"]
                answer = True
            elif op == "release":
                gates[request["gate"]].set()
                answer = True
            elif op == "wait":
                with cv:
                    ok = cv.wait_for(lambda: any(r["path"] == request["path"] for r in trace), 60)
                    if not ok:
                        raise TimeoutError("request not observed: " + request["path"])
                answer = True
            elif op == "trace":
                with cv:
                    answer = list(trace)
            elif op == "stop":
                print(json.dumps(True), flush=True)
                break
            else:
                raise ValueError(op)
            print(json.dumps(answer), flush=True)
    finally:
        for gate in gates.values():
            gate.set()
        server.shutdown()
        server.server_close()
        worker.join()


class RemoteSource:
    def __init__(self, kind, size, conflict):
        self.process = subprocess.Popen(
            [sys.executable, "-B", str(Path(__file__).resolve()), "_serve", kind, str(size), str(int(conflict))],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        try:
            self.url = json.loads(self.process.stdout.readline())["url"]
        except BaseException:
            self.close()
            raise

    def call(self, op, **values):
        self.process.stdin.write(json.dumps(dict(op=op, **values)) + "\n")
        self.process.stdin.flush()
        line = self.process.stdout.readline()
        if not line:
            raise RuntimeError("source exited: %r" % self.process.poll())
        return json.loads(line)

    def close(self):
        try:
            if self.process.poll() is None:
                self.call("stop")
                self.process.wait(timeout=100)
        finally:
            if self.process.poll() is None:
                self.process.kill()
                self.process.wait()
            for pipe in (self.process.stdin, self.process.stdout):
                pipe.close()
        if self.process.returncode:
            raise RuntimeError("source exit %s" % self.process.returncode)


def verify_db(path, expected):
    with sqlite3.connect(path) as db:
        positions = db.execute("SELECT sequence,discontinuity FROM positions ORDER BY sequence").fetchall()
        assert positions == [(seq, 0) for seq in sorted(expected)], positions
        rows = db.execute("SELECT sequence,sha256,length(body) FROM segments ORDER BY sequence").fetchall()
        assert [row[0] for row in rows] == sorted(expected), rows
        for seq, digest, length in rows:
            spec = expected[seq]
            assert length == sum(n for _, n in spec)
            hasher = hashlib.sha256()
            with db.blobopen("segments", "body", seq, readonly=True) as blob:
                for wanted in chunks(spec):
                    actual = blob.read(len(wanted))
                    assert actual == wanted, ("bytes differ", seq, blob.tell())
                    hasher.update(actual)
                assert blob.read(1) == b""
            assert hasher.hexdigest() == digest, ("digest differs", seq)


def measure(kind, size, conflict=False):
    from livestore import Archive, Collector, Position
    source = RemoteSource(kind, size, conflict)
    _, expected = workload(kind, size)
    old_tempdir = tempfile.tempdir
    with tempfile.TemporaryDirectory(prefix="livestore-memory-") as root:
        root = Path(root)
        scratch = root / "scratch"
        scratch.mkdir()
        tempfile.tempdir = str(scratch)
        path = root / "archive.sqlite"
        archive = Archive(path)
        collector = Collector(source.url, archive, workers=4, timeout=90)
        rejected = False
        peak = None
        try:
            gc.collect()
            tracemalloc.start()
            try:
                collector.poll()
                if kind == "long":
                    status = collector.settle(timeout=90)
                    assert status["complete"], status
                    collector.poll()
                else:
                    collector.wait_for_download(Position(40, 0), 0, timeout=90)
                    status = collector.wait_for_download(Position(40, 0), 1, timeout=90)
                    assert [p["position"] for p in status["pending"]] == [[0, 40]], status
                    source.call("advance", stage=1)
                    collector.poll()
                    collector.wait_for_download(Position(41, 0), 0, timeout=90)
                    collector.wait_for_download(Position(42, 0), timeout=90)
                    source.call("wait", path="/d")
                    source.call("wait", path="/late")
                    source.call("advance", stage=2)
                    collector.poll()
                    source.call("release", gate="part")
                    status = collector.wait_for_download(Position(41, 0), 1, timeout=90)
                    assert [p["position"] for p in status["pending"]] == [[0, 40]], status
                    source.call("advance", stage=3)
                    status = collector.poll()
                    assert status["complete"], status
                    source.call("release", gate="late")
                    try:
                        status = collector.settle(timeout=90)
                    except ValueError:
                        if not conflict:
                            raise
                        rejected = True
                    assert rejected == conflict, ("late conflict not rejected", status)
                if not conflict:
                    for _ in range(3):
                        assert collector.status()["complete"]
            finally:
                try:
                    collector.close()
                except ValueError:
                    if not (conflict and rejected):
                        raise
                finally:
                    peak = tracemalloc.get_traced_memory()[1]
                    tracemalloc.stop()
                    archive.close()
            # Explicit read and independent byte accounting are outside measurement.
            verify_db(path, expected)
            trace = source.call("trace")
            if kind == "late":
                for uri in ("/a", "/b", "/c", "/d", "/late"):
                    assert sum(r["path"] == uri for r in trace) == 1, trace
                assert not any(r["path"] == "/expired" for r in trace), trace
            else:
                assert sum(r["path"] != "/live.m3u8" for r in trace) == 4, trace
            remaining = sorted(str(p.relative_to(root)) for p in root.rglob("*") if p.is_file())
            assert remaining == ["archive.sqlite"], ("staging not released", remaining)
            with Archive(path) as reader:
                if size <= MIB:
                    snapshot = reader.read()
                    assert [(s.position.sequence, s.body) for s in snapshot.segments] == [
                        (seq, b"".join(chunks(expected[seq]))) for seq in sorted(expected)]
                seq = min(expected)
                with reader.db.blobopen("segments", "body", seq) as blob:
                    first = blob.read(1)
                    blob.seek(0)
                    blob.write(bytes([first[0] ^ 1]))
                checker = Collector(source.url, reader, workers=4)
                try:
                    try:
                        checker.status()
                    except ValueError:
                        pass
                    else:
                        raise AssertionError("status failed to detect stored digest corruption")
                finally:
                    checker.close()
            return {"kind": kind, "size": size, "peak": peak,
                    "conflict_rejected": rejected, "requests": len(trace)}
        finally:
            tempfile.tempdir = old_tempdir
            source.close()


def child_measure(kind, size, conflict=False):
    process = subprocess.run(
        [sys.executable, "-B", str(Path(__file__).resolve()), "_measure", kind, str(size), str(int(conflict))],
        text=True, stdout=subprocess.PIPE, check=True)
    return json.loads(process.stdout)



def deferred_conflict():
    # A partial representation is not a complete conflicting publication.
    from livestore import Archive, Collector, Position
    from livestore.source import Recording, Stage, Reply
    text = playlist(7, part("/p0") + part("/p1") + full("/whole", 2), True)
    source = Recording([Stage(text, {
        "/p0": (Reply(b"X"),), "/p1": (Reply(b"B", gate="tail"),),
        "/whole": (Reply(b"AB"),)})], gate_timeout=60)
    archive = Archive()
    collector = Collector(source.url, archive, workers=4)
    rejected = False
    try:
        collector.poll()
        collector.wait_for_download(Position(7, 0), timeout=30)
        collector.wait_for_download(Position(7, 0), 0, timeout=30)
        assert archive.read().segments[0].body == b"AB"
        source.release("tail")
        try:
            collector.settle(timeout=30)
        except ValueError:
            rejected = True
        assert rejected, "complete late alternative was not checked"
        assert archive.read().segments[0].body == b"AB"
    finally:
        source.close()
        try:
            collector.close()
        except ValueError:
            if not rejected:
                raise
        finally:
            archive.close()
    return {"deferred_conflict": "old bytes preserved"}


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "smoke"
    if mode == "_serve":
        serve(sys.argv[2], int(sys.argv[3]), bool(int(sys.argv[4])))
        return
    if mode == "_measure":
        print(json.dumps(measure(sys.argv[2], int(sys.argv[3]), bool(int(sys.argv[4])))))
        return
    if mode not in ("smoke", "baseline", "target"):
        raise SystemExit("usage: memory_bench.py [smoke|baseline|target]")
    if mode in ("baseline", "target"):
        subprocess.run([sys.executable, "-B", "-m", "unittest", "discover",
                        "-s", "tests", "-t", ".", "-v"], check=True)
    print(json.dumps(deferred_conflict()), flush=True)
    small = [child_measure("long", MIB), child_measure("late", MIB),
             child_measure("late", MIB, True)]
    print(json.dumps({"small": small}), flush=True)
    if mode == "smoke":
        return
    report = []
    for kind in ("long", "late"):
        medians = []
        for size in (32 * MIB, 128 * MIB):
            rounds = [child_measure(kind, size) for _ in range(3)]
            print(json.dumps({"kind": kind, "size": size, "rounds": rounds}), flush=True)
            peaks = [r["peak"] for r in rounds]
            medians.append(statistics.median(peaks))
            report.extend(rounds)
            if mode == "target":
                assert max(peaks) <= 24 * MIB, ("peak exceeds 24MiB", rounds)
        if mode == "target":
            assert medians[1] - medians[0] <= 4 * MIB, ("growth exceeds 4MiB", kind, medians)
    print(json.dumps({"mode": mode, "measurements": report, "result": "business checks passed"}))


if __name__ == "__main__":
    main()
