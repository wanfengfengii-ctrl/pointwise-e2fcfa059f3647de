"""SQLite publication and independent ordered archive reads."""
from dataclasses import dataclass
import hashlib
import sqlite3
from .playlist import Position


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


class Archive:
    """Single writer connection; other Archive instances may read the same file."""

    def __init__(self, path=":memory:"):
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA foreign_keys=ON")
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
