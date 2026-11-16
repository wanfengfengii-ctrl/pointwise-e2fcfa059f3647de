"""Disk-backed payload staging for large media representations.

Download workers stream HTTP response bodies into files under a session
owned directory instead of accumulating whole bodies in Python memory.
A ticket records the immutable binding (position/part), the byte length
and the SHA-256 digest computed while streaming. Files are released as
soon as publication or alternative verification stops depending on them
and the whole directory is reclaimed when the collector closes.
"""
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import shutil
import tempfile


@dataclass(frozen=True, slots=True)
class Ticket:
    position: object
    part: int | None
    path: Path
    size: int
    digest: str

    @property
    def token(self):
        return (self.position, self.part)


class Staging:
    """Session scoped directory of streamed payloads.

    The directory is created under the process default temporary
    directory (tests and benchmarks point tempfile.tempdir at a scratch
    root they own) and is removed wholesale by close().
    """

    def __init__(self, prefix="livestore-stage-"):
        self._root = Path(tempfile.mkdtemp(prefix=prefix))
        self._serial = 0
        self.closed = False
        # Fault injection points used by the staging failure regression.
        # write_fault(ticket_path, chunk_index) may raise OSError.
        self.write_fault = None
        # read_fault(ticket_path) may raise OSError when re-reading.
        self.read_fault = None

    @property
    def root(self):
        return self._root

    def _new_path(self, position, part):
        self._serial += 1
        name = "p%d-d%d-%s-%d.tmp" % (
            position.sequence, position.discontinuity,
            "full" if part is None else "part%d" % part, self._serial)
        return self._root / name

    def store(self, position, part, chunks, expected_length=None):
        """Stream an iterable of bytes chunks to disk.

        Returns a Ticket on success. Any failure removes the partial
        file so no half representation becomes visible. The expected
        length, when known from Content-Length, is enforced so HTTP
        short reads never become short successful segments.
        """
        if self.closed:
            raise RuntimeError("staging closed")
        path = self._new_path(position, part)
        hasher = hashlib.sha256()
        length = 0
        index = 0
        try:
            with open(path, "wb") as handle:
                for chunk in chunks:
                    if not chunk:
                        continue
                    if self.write_fault is not None:
                        self.write_fault(path, index)
                    handle.write(chunk)
                    hasher.update(chunk)
                    length += len(chunk)
                    index += 1
            if expected_length is not None and length != expected_length:
                raise OSError("incomplete transfer: expected %d bytes, got %d"
                              % (expected_length, length))
        except BaseException:
            self._discard(path)
            raise
        return Ticket(position, part, path, length, hasher.hexdigest())

    def open_read(self, ticket):
        if self.read_fault is not None:
            self.read_fault(ticket.path)
        return open(ticket.path, "rb")

    def release(self, ticket):
        """Release a payload once publication/verification no longer needs it."""
        self._discard(ticket.path)

    @staticmethod
    def _discard(path):
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        except OSError:
            pass

    def close(self):
        """Reclaim every payload owned by this session, including orphans."""
        if self.closed:
            return
        self.closed = True
        shutil.rmtree(self._root, ignore_errors=True)
