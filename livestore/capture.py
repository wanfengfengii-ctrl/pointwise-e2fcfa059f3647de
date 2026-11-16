"""Polling, immutable download bindings, and complete-coverage acceptance."""
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass
from http.client import HTTPException
from urllib.parse import urljoin, urlsplit, urlunsplit, parse_qsl, urlencode
from urllib.request import urlopen
from .playlist import Session, PlaylistError, Position


@dataclass(frozen=True)
class Binding:
    position: Position
    part: int | None
    uri: str

    @property
    def token(self):
        return (self.position, self.part)


class Collector:
    """Call control methods on one thread. Workers perform only HTTP reads.

    poll schedules work; settle waits only for work already scheduled.
    Re-poll to retry failures. close joins outstanding HTTP operations.
    A new collector starts a new session, not a resumable capture.
    """

    def __init__(self, playlist_url, archive, workers=8, timeout=10):
        if workers < 1 or timeout <= 0:
            raise ValueError("positive workers and timeout required")
        self.url = playlist_url
        self.archive = archive
        self.timeout = timeout
        self.session = Session()
        self._pool = ThreadPoolExecutor(max_workers=workers)
        self._futures = {}
        self._active = set()
        self._bodies = {}
        self._errors = {}
        self._playlist_error = None
        self._need_full = False
        self._closed = False

    def _get(self, uri):
        if urlsplit(uri).scheme not in ("http", "https"):
            raise ValueError("media transport requires HTTP(S)")
        with urlopen(uri, timeout=self.timeout) as response:
            return response.read()

    def _playlist_url(self, delta):
        parsed = urlsplit(self.url)
        query = [(k, v) for k, v in parse_qsl(parsed.query) if k != "_HLS_skip"]
        if delta:
            query.append(("_HLS_skip", "YES"))
        return urlunsplit(parsed._replace(query=urlencode(query)))

    def poll(self):
        if self._closed:
            raise RuntimeError("collector closed")
        self._drain()
        try:
            text = self._get(self._playlist_url(True)).decode("utf-8")
            update = self.session.accept_playlist(text)
            self._need_full = update.need_full
            if update.need_full:
                text = self._get(self._playlist_url(False)).decode("utf-8")
                update = self.session.accept_playlist(text)
                self._need_full = update.need_full
            if update.need_full:
                self._playlist_error = "full playlist still lacks context"
            else:
                self._playlist_error = None
        except (OSError, ValueError, HTTPException) as exc:
            self._playlist_error = type(exc).__name__ + ": " + str(exc)
        self._reconcile()
        self._schedule()
        self._drain()
        return self.status()

    def _schedule(self):
        for declaration in self.session.declarations:
            if self.archive.contains(declaration.position):
                continue
            bindings = [Binding(declaration.position, i, part.uri)
                        for i, part in enumerate(declaration.parts)]
            if declaration.uri is not None:
                bindings.append(Binding(declaration.position, None, declaration.uri))
            for binding in bindings:
                token = binding.token
                if token not in self._bodies and token not in self._active:
                    self._active.add(token)
                    future = self._pool.submit(self._get, urljoin(self.url, binding.uri))
                    self._futures[future] = binding

    def _drain(self):
        for future in list(self._futures):
            if not future.done():
                continue
            binding = self._futures.pop(future)
            self._active.remove(binding.token)
            try:
                self._bodies[binding.token] = future.result()
                self._errors.pop(binding.token, None)
            except Exception as exc:
                self._errors[binding.token] = type(exc).__name__ + ": " + str(exc)
        self._reconcile()

    def _reconcile(self):
        declarations = self.session.declarations
        self.archive.declare(declarations)
        for declaration in declarations:
            p = declaration.position
            full = self._bodies.get((p, None))
            if full is not None:
                self.archive.publish(p, full)
            count = declaration.sealed_parts
            if count is not None and all((p, i) in self._bodies for i in range(count)):
                body = b"".join(self._bodies[p, i] for i in range(count))
                self.archive.publish(p, body)
            if not self.archive.contains(p):
                failures = [message for (pos, _), message in self._errors.items() if pos == p]
                if failures:
                    reason = "download failed: " + "; ".join(sorted(set(failures)))
                elif declaration.uri is None:
                    reason = "tail completeness unproven"
                else:
                    reason = "awaiting complete coverage"
                self.archive.pending(p, reason)

    def settle(self, timeout=5):
        if self._closed:
            raise RuntimeError("collector closed")
        _, unfinished = wait(tuple(self._futures), timeout=timeout)
        self._drain()
        if unfinished:
            raise TimeoutError("media requests still in flight")
        return self.status()

    def wait_for_download(self, position, part=None, timeout=5):
        """Wait for one scheduled representation without blocking other responses."""
        token = (position, part)
        matches = [future for future, binding in self._futures.items() if binding.token == token]
        if matches:
            _, unfinished = wait(matches, timeout=timeout)
            self._drain()
            if unfinished:
                raise TimeoutError("selected media request still in flight")
        elif token not in self._bodies and token not in self._errors:
            raise KeyError("representation was not scheduled")
        return self.status()

    def status(self):
        snapshot = self.archive.read()
        missing = self.session.missing_ranges()
        return {
            "complete": bool(self.session.ended and not snapshot.pending and not missing
                             and self._playlist_error is None and not self._need_full),
            "need_full": self._need_full,
            "playlist_error": self._playlist_error,
            "pending": [{"position": list(p.position.identity), "reason": p.reason}
                        for p in snapshot.pending],
            "missing_ranges": [list(r) for r in missing],
            "in_flight": len(self._futures),
            "download_errors": [
                {"position": list(p.identity), "part": i, "error": message}
                for (p, i), message in sorted(
                    self._errors.items(), key=lambda item: (item[0][0], -1 if item[0][1] is None else item[0][1]))
            ],
        }

    def close(self):
        if not self._closed:
            self._pool.shutdown(wait=True, cancel_futures=True)
            self._drain()
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
