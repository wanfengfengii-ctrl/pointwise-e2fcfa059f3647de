"""Actual loopback HTTP recording server with explicit stages and response gates."""
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit, parse_qs
import json
import threading
import time


@dataclass(frozen=True)
class Reply:
    body: bytes = b""
    status: int = 200
    gate: str | None = None


@dataclass(frozen=True)
class Stage:
    playlist: str
    resources: dict[str, tuple[Reply, ...]] = field(default_factory=dict)
    full_playlist: str | None = None


class Recording:
    """Stage changes affect future requests only, never a captured Reply."""

    def __init__(self, stages, gate_timeout=5):
        if not stages:
            raise ValueError("recording needs a stage")
        self.stages = tuple(stages)
        self.gate_timeout = gate_timeout
        self._stage = 0
        self._condition = threading.Condition()
        self._attempts = {}
        self._trace = []
        self._gates = {}
        self._serial = 0
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                parsed = urlsplit(self.path)
                with owner._condition:
                    stage_index = owner._stage
                    stage = owner.stages[stage_index]
                    owner._serial += 1
                    request_id = owner._serial
                    if parsed.path == "/live.m3u8":
                        delta = parse_qs(parsed.query).get("_HLS_skip") == ["YES"]
                        text = stage.playlist if delta else (
                            stage.full_playlist if stage.full_playlist is not None else stage.playlist)
                        reply = Reply(text.encode("utf-8"))
                    else:
                        key = (stage_index, parsed.path)
                        attempt = owner._attempts.get(key, 0)
                        owner._attempts[key] = attempt + 1
                        choices = stage.resources.get(parsed.path, ())
                        reply = choices[min(attempt, len(choices) - 1)] if choices else Reply(b"expired", 410)
                    gate = None
                    if reply.gate:
                        gate = owner._gates.setdefault(reply.gate, threading.Event())
                    owner._trace.append({"event": "request", "id": request_id,
                                         "stage": stage_index, "path": parsed.path,
                                         "query": parsed.query})
                    owner._condition.notify_all()
                status = reply.status
                body = reply.body
                if gate is not None and not gate.wait(owner.gate_timeout):
                    status, body = 504, b"recording gate timed out"
                delivered = False
                try:
                    self.send_response(status)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    delivered = True
                except (BrokenPipeError, ConnectionResetError):
                    pass
                finally:
                    with owner._condition:
                        owner._trace.append({"event": "response", "id": request_id,
                                             "stage": stage_index, "path": parsed.path,
                                             "status": status, "bytes": len(body),
                                             "delivered": delivered})
                        owner._condition.notify_all()

            def log_message(self, *args):
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = False
        self._thread = threading.Thread(target=self._server.serve_forever,
                                        kwargs={"poll_interval": 0.02}, name="recorded-http")
        self._thread.start()
        self.url = "http://127.0.0.1:%s/live.m3u8" % self._server.server_address[1]

    @classmethod
    def from_json(cls, path):
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        stages = []
        for item in data["stages"]:
            resources = {}
            for uri, replies in item.get("resources", {}).items():
                resources[uri] = tuple(Reply(
                    bytes.fromhex(r["body_hex"]) if "body_hex" in r else r.get("body", "").encode("utf-8"),
                    r.get("status", 200), r.get("gate")) for r in replies)
            stages.append(Stage(item["playlist"], resources, item.get("full_playlist")))
        return cls(stages)

    def advance(self, stage):
        with self._condition:
            if not self._stage <= stage < len(self.stages):
                raise ValueError("stages only move forward within the recording")
            self._stage = stage

    def release(self, name):
        with self._condition:
            self._gates.setdefault(name, threading.Event()).set()

    def wait_requests(self, path, count=1, timeout=3):
        deadline = time.monotonic() + timeout
        with self._condition:
            while sum(r["event"] == "request" and r["path"] == path for r in self._trace) < count:
                left = deadline - time.monotonic()
                if left <= 0:
                    raise TimeoutError("expected request not observed: " + path)
                self._condition.wait(left)

    def wait_responses(self, path, count=1, timeout=3):
        deadline = time.monotonic() + timeout
        with self._condition:
            while sum(r["event"] == "response" and r["path"] == path for r in self._trace) < count:
                left = deadline - time.monotonic()
                if left <= 0:
                    raise TimeoutError("expected response not observed: " + path)
                self._condition.wait(left)

    @property
    def trace(self):
        with self._condition:
            return tuple(dict(row) for row in self._trace)

    def close(self):
        with self._condition:
            for gate in self._gates.values():
                gate.set()
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
