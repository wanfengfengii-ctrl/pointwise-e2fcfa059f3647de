"""Polling, immutable download bindings, and complete-coverage acceptance.

Media responses are streamed into session owned disk staging (see
staging.py) in bounded chunks; whole payloads never accumulate in Python
memory. Reconciliation streams staged representations into SQLite and
verifies late alternative representations byte for byte against the
already published body.
"""
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass
from http.client import HTTPException
from urllib.parse import urljoin, urlsplit, urlunsplit, parse_qsl, urlencode
from urllib.request import urlopen
from .playlist import Session, PlaylistError, Position
from .staging import Staging

CHUNK = 1 << 16


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
        # token -> staging Ticket for successfully downloaded representations
        self._tickets = {}
        # tokens whose representation was downloaded and fully accepted
        # (published or verified against an existing publication); the
        # payload itself has already been released
        self._done = set()
        self._errors = {}
        self._playlist_error = None
        self._need_full = False
        self._closed = False
        self._staging = Staging()

    def _open_playlist(self, uri):
        if urlsplit(uri).scheme not in ("http", "https"):
            raise ValueError("media transport requires HTTP(S)")
        with urlopen(uri, timeout=self.timeout) as response:
            return response.read()

    def _download(self, binding):
        """Worker entry point: stream one response body to disk staging."""
        uri = urljoin(self.url, binding.uri)
        if urlsplit(uri).scheme not in ("http", "https"):
            raise ValueError("media transport requires HTTP(S)")

        def blocks(response):
            while True:
                chunk = response.read(CHUNK)
                if not chunk:
                    break
                yield chunk

        with urlopen(uri, timeout=self.timeout) as response:
            length = response.length  # Content-Length when the server sent it
            return self._staging.store(binding.position, binding.part,
                                       blocks(response), length)

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
            text = self._open_playlist(self._playlist_url(True)).decode("utf-8")
            update = self.session.accept_playlist(text)
            self._need_full = update.need_full
            if update.need_full:
                text = self._open_playlist(self._playlist_url(False)).decode("utf-8")
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
                if token not in self._tickets and token not in self._active and token not in self._done:
                    self._active.add(token)
                    # A fresh attempt supersedes the previous failure record.
                    self._errors.pop(token, None)
                    future = self._pool.submit(self._download, binding)
                    self._futures[future] = binding

    def _drain(self):
        for future in list(self._futures):
            if not future.done():
                continue
            binding = self._futures.pop(future)
            self._active.remove(binding.token)
            try:
                ticket = future.result()
            except Exception as exc:
                self._errors[binding.token] = type(exc).__name__ + ": " + str(exc)
            else:
                self._tickets[binding.token] = ticket
                self._errors.pop(binding.token, None)
        self._reconcile()

    def _publish(self, position, tickets):
        """Stream staged tickets into the archive; release them on success."""
        self.archive.publish_files(position, tickets, self._staging.open_read)
        # Reached only when the transaction committed (or an exact match was
        # verified): a failure rolls back, leaves no half segment visible and
        # keeps the staged candidates available for a later retry.
        for ticket in tickets:
            self._tickets.pop(ticket.token, None)
            self._done.add(ticket.token)
            self._staging.release(ticket)

    def _reconcile(self):
        declarations = self.session.declarations
        self.archive.declare(declarations)
        known = {declaration.position: declaration for declaration in declarations}
        handled = set()

        def accept(position, tickets):
            self._publish(position, tickets)
            handled.update(ticket.token for ticket in tickets)

        for declaration in declarations:
            p = declaration.position
            full = self._tickets.get((p, None))
            if full is not None:
                accept(p, [full])
            count = declaration.sealed_parts
            if count is not None:
                parts = [self._tickets.get((p, i)) for i in range(count)]
                if all(ticket is not None for ticket in parts):
                    accept(p, parts)
            if not self.archive.contains(p):
                failures = [message for (pos, _), message in self._errors.items() if pos == p]
                if failures:
                    reason = "download failed: " + "; ".join(sorted(set(failures)))
                elif declaration.uri is None:
                    reason = "tail completeness unproven"
                else:
                    reason = "awaiting complete coverage"
                self.archive.pending(p, reason)

        # A representation that finished after its position was published,
        # or whose declaration has already slid out of the window, is still
        # accepted and verified byte for byte: never digest-only and never
        # skipped just because publication exists. An exact match releases
        # the staging payload; a conflict raises while the original
        # publication stays intact.
        for token, ticket in list(self._tickets.items()):
            if token in handled:
                continue
            p, part = token
            declaration = known.get(p)
            if part is not None:
                # Part payloads only count once the part set is proven
                # complete; a lone prefix is never a full alternative and
                # must stay staged for a later proof or conflict check.
                if declaration is None:
                    continue
                count = declaration.sealed_parts
                if count is None:
                    continue
                parts = [self._tickets.get((p, i)) for i in range(count)]
                if all(candidate is not None for candidate in parts):
                    accept(p, parts)
            else:
                # Full segments are complete representations even when the
                # declaration has slid out of the current window.
                accept(p, [ticket])

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
        elif token not in self._tickets and token not in self._errors and token not in self._done:
            raise KeyError("representation was not scheduled")
        return self.status()

    def status(self):
        overview = self.archive.inspect()
        missing = self.session.missing_ranges()
        return {
            "complete": bool(self.session.ended and not overview.pending and not missing
                             and self._playlist_error is None and not self._need_full),
            "need_full": self._need_full,
            "playlist_error": self._playlist_error,
            "pending": [{"position": list(p.position.identity), "reason": p.reason}
                        for p in overview.pending],
            "missing_ranges": [list(r) for r in missing],
            "in_flight": len(self._futures),
            "download_errors": [
                {"position": list(p.identity), "part": i, "error": message}
                for (p, i), message in sorted(
                    self._errors.items(), key=lambda item: (item[0][0], -1 if item[0][1] is None else item[0][1]))
            ],
        }

    def close(self):
        if self._closed:
            return
        try:
            self._pool.shutdown(wait=True, cancel_futures=True)
            try:
                self._drain()
            finally:
                self._closed = True
        finally:
            # Reclaim this session's staging, including responses whose
            # acceptance never ran; never touch caller owned files.
            self._staging.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
