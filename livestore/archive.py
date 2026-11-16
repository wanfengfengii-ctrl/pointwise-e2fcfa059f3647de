"""SQLite publication and independent ordered archive reads."""
from dataclasses import dataclass
import hashlib
import os
import sqlite3
from .playlist import Position

CHUNK = 1 << 16


@dataclass(frozen=True)
class Segment:
    position: Position
    body: bytes
    sha256: str


@dataclass(frozen=True)
class Pending:
    position: Position
    reason: str


@dataclass(frozen=True)
class Snapshot:
    segments: tuple[Segment, ...]
    pending: tuple[Pending, ...]


@dataclass(frozen=True)
class Overview:
    """Position view used by status(): no media bodies are loaded."""
    published: tuple[Position, ...]
    pending: tuple[Pending, ...]


class Archive:
    """Single writer connection; other Archive instances may read the same file."""

    def __init__(self, path=":memory:"):
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA foreign_keys=ON")
        # Test seam: when set, called inside the publish transaction right
        # before commit; raising forces a rollback of the whole publication.
        self.commit_fault = None
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS positions(
                sequence INTEGER PRIMARY KEY, discontinuity INTEGER NOT NULL,
                reason TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS segments(
                sequence INTEGER PRIMARY KEY REFERENCES positions(sequence),
                body BLOB NOT NULL, sha256 TEXT NOT NULL);
            PRAGMA user_version=1;
        """)

    def declare(self, declarations):
        with self.db:
            for declaration in declarations:
                p = declaration.position
                old = self.db.execute(
                    "SELECT discontinuity FROM positions WHERE sequence=?", (p.sequence,)).fetchone()
                if old is not None and old[0] != p.discontinuity:
                    raise ValueError("archive sequence belongs to another discontinuity")
                self.db.execute("INSERT OR IGNORE INTO positions VALUES(?,?,?)",
                                (p.sequence, p.discontinuity, "awaiting coverage"))

    def contains(self, position):
        row = self.db.execute("""
            SELECT p.discontinuity FROM segments s JOIN positions p USING(sequence)
            WHERE sequence=?""", (position.sequence,)).fetchone()
        return row is not None and row[0] == position.discontinuity

    def publish(self, position, body):
        """In-memory publication path retained for the original public contract."""
        if not isinstance(body, bytes):
            raise TypeError("publication requires bytes")
        digest = hashlib.sha256(body).hexdigest()
        with self.db:
            row = self.db.execute("SELECT discontinuity FROM positions WHERE sequence=?",
                                  (position.sequence,)).fetchone()
            if row is None or row[0] != position.discontinuity:
                raise ValueError("undeclared position")
            old = self.db.execute("SELECT body,sha256 FROM segments WHERE sequence=?",
                                  (position.sequence,)).fetchone()
            if old is not None:
                if old != (body, digest):
                    raise ValueError("conflicting complete media at one position")
                return False
            self.db.execute("INSERT INTO segments VALUES(?,?,?)", (position.sequence, body, digest))
        return True

    def publish_files(self, position, tickets, opener):
        """Stream one complete representation from staging tickets into SQLite.

        A single ticket publishes a complete segment; several tickets in
        part order publish a proven complete part set. Payloads stay on
        disk and only bounded chunks cross into Python.

        A representation at an already published position is verified byte
        for byte against the stored body (never digest-only); conflicts
        raise, preserving the original publication. Any failure rolls the
        transaction back, so a half-written segment is never visible and
        the staging candidates remain retryable.
        """
        sizes = [ticket.size for ticket in tickets]
        digests = [ticket.digest for ticket in tickets]
        openers = [lambda ticket=ticket: opener(ticket) for ticket in tickets]
        with self.db:
            row = self.db.execute("SELECT discontinuity FROM positions WHERE sequence=?",
                                  (position.sequence,)).fetchone()
            if row is None or row[0] != position.discontinuity:
                raise ValueError("undeclared position")
            old = self.db.execute("SELECT sha256 FROM segments WHERE sequence=?",
                                  (position.sequence,)).fetchone()
            if old is not None:
                # sequence is the INTEGER PRIMARY KEY, hence also the rowid;
                # the stored blob is streamed through blobopen so verifying a
                # late alternative never materializes the published body.
                with self.db.blobopen("segments", "body", position.sequence,
                                      readonly=True) as stored:
                    self._verify_files(stored, old[0], openers, digests, sum(sizes))
                return False
            total = sum(sizes)
            cursor = self.db.execute(
                "INSERT INTO segments(sequence,body,sha256) VALUES(?,zeroblob(?),?)",
                (position.sequence, total, ""))
            rowid = cursor.lastrowid
            hasher = hashlib.sha256()
            with self.db.blobopen("segments", "body", rowid) as blob:
                for ticket_open in openers:
                    with ticket_open() as handle:
                        while True:
                            chunk = handle.read(CHUNK)
                            if not chunk:
                                break
                            blob.write(chunk)
                            hasher.update(chunk)
            digest = hasher.hexdigest()
            self.db.execute("UPDATE segments SET sha256=? WHERE sequence=?",
                            (digest, position.sequence))
            if self.commit_fault is not None:
                self.commit_fault(position)
        return True

    @staticmethod
    def _verify_files(stored_blob, stored_digest, openers, digests, candidate_size):
        """Byte-for-byte alternative check using only bounded chunks.

        Both the stored publication and the staged candidates are streamed;
        the stored digest and every staged ticket digest are re-derived
        rather than trusted. Any mismatch raises before returning, leaving
        the original publication untouched.
        """
        stored_blob.seek(0, os.SEEK_END)
        stored_length = stored_blob.tell()
        stored_blob.seek(0)
        if stored_length != candidate_size:
            raise ValueError("conflicting complete media at one position")
        stored_hasher = hashlib.sha256()
        offset = 0
        for ticket_open, expected_digest in zip(openers, digests):
            ticket_hasher = hashlib.sha256()
            with ticket_open() as handle:
                while True:
                    chunk = handle.read(CHUNK)
                    if not chunk:
                        break
                    ticket_hasher.update(chunk)
                    stored_chunk = stored_blob.read(len(chunk))
                    if stored_chunk != chunk:
                        raise ValueError("conflicting complete media at one position")
                    stored_hasher.update(stored_chunk)
                    offset += len(chunk)
            if ticket_hasher.hexdigest() != expected_digest:
                raise ValueError("conflicting complete media at one position")
        if offset != stored_length or stored_blob.read(1) != b"":
            raise ValueError("conflicting complete media at one position")
        if stored_hasher.hexdigest() != stored_digest:
            raise ValueError("stored media digest mismatch")

    def inspect(self):
        """Ordered positions and pending reasons without reading bodies.

        Stored digests are still checked through streaming blob reads, so
        corruption is detected without materializing full media.
        """
        rows = self.db.execute("""
            SELECT p.sequence,p.discontinuity,s.sha256,p.reason
            FROM positions p LEFT JOIN segments s USING(sequence) ORDER BY p.sequence
        """).fetchall()
        published, pending = [], []
        for seq, disc, digest, reason in rows:
            position = Position(seq, disc)
            if digest is None:
                pending.append(Pending(position, reason))
            else:
                with self.db.blobopen("segments", "body", seq, readonly=True) as blob:
                    hasher = hashlib.sha256()
                    while True:
                        chunk = blob.read(CHUNK)
                        if not chunk:
                            break
                        hasher.update(chunk)
                    if hasher.hexdigest() != digest:
                        raise ValueError("stored media digest mismatch")
                published.append(position)
        return Overview(tuple(published), tuple(pending))

    def pending(self, position, reason):
        with self.db:
            self.db.execute("UPDATE positions SET reason=? WHERE sequence=? AND discontinuity=?",
                            (reason, position.sequence, position.discontinuity))

    def read(self):
        rows = self.db.execute("""
            SELECT p.sequence,p.discontinuity,s.body,s.sha256,p.reason
            FROM positions p LEFT JOIN segments s USING(sequence) ORDER BY p.sequence
        """).fetchall()
        segments, pending = [], []
        for seq, disc, body, digest, reason in rows:
            position = Position(seq, disc)
            if body is None:
                pending.append(Pending(position, reason))
            else:
                if hashlib.sha256(body).hexdigest() != digest:
                    raise ValueError("stored media digest mismatch")
                segments.append(Segment(position, body, digest))
        return Snapshot(tuple(segments), tuple(pending))

    def close(self):
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
