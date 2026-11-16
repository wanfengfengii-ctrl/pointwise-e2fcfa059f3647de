"""Finite media-playlist grammar and transactional historical continuation."""
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
from fractions import Fraction
import re


class PlaylistError(ValueError):
    pass


@dataclass(frozen=True, order=True)
class Position:
    sequence: int
    discontinuity: int

    @property
    def identity(self):
        return (self.discontinuity, self.sequence)


@dataclass(frozen=True)
class Part:
    uri: str
    duration: Fraction


@dataclass(frozen=True)
class Declaration:
    position: Position
    parts: tuple[Part, ...] = ()
    uri: str | None = None
    duration: Fraction | None = None

    @property
    def sealed_parts(self):
        if self.duration is not None and self.parts:
            if sum((p.duration for p in self.parts), Fraction(0)) == self.duration:
                return len(self.parts)
        return None


@dataclass(frozen=True)
class Update:
    need_full: bool
    declarations: tuple[Declaration, ...]
    ended: bool


@dataclass(frozen=True)
class _Raw:
    base: int
    disc: int
    skip: int
    entries: tuple
    ended: bool


_ATTR = re.compile(r'([A-Z0-9-]+)=("[^"\r\n]*"|[^,\s]+)(?:,|$)')


def attributes(text):
    if not text or text.endswith(","):
        raise PlaylistError("empty or trailing attributes")
    result = {}
    at = 0
    while at < len(text):
        match = _ATTR.match(text, at)
        if not match or match[1] in result:
            raise PlaylistError("invalid or duplicate attributes")
        value = match[2]
        result[match[1]] = value[1:-1] if value.startswith('"') else value
        at = match.end()
    return result


def _integer(value):
    if not re.fullmatch(r"[0-9]+", value):
        raise PlaylistError("expected nonnegative integer")
    number = int(value)
    if number > 2**63 - 1:
        raise PlaylistError("integer outside SQLite range")
    return number


def _duration(value):
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise PlaylistError("invalid duration") from exc
    if not number.is_finite() or number <= 0:
        raise PlaylistError("duration must be finite and positive")
    return Fraction(number)


def _uri(value):
    if not value or any(c.isspace() for c in value):
        raise PlaylistError("invalid URI")
    return value


def parse(text):
    if not isinstance(text, str):
        raise PlaylistError("playlist must be text")
    lines = [s.strip() for s in text.splitlines() if s.strip()]
    if not lines or lines[0] != "#EXTM3U":
        raise PlaylistError("missing EXTM3U")
    base = disc = skip = 0
    headers = set()
    entries = []
    parts = []
    duration = None
    offset = 0
    ended = False
    media_started = False
    for line in lines[1:]:
        if ended:
            raise PlaylistError("content after ENDLIST")
        if line.startswith(("#EXT-X-MEDIA-SEQUENCE:", "#EXT-X-DISCONTINUITY-SEQUENCE:", "#EXT-X-SKIP:")):
            name, value = line.split(":", 1)
            if media_started or name in headers:
                raise PlaylistError("late or duplicate sequence header")
            headers.add(name)
            if name == "#EXT-X-MEDIA-SEQUENCE":
                base = _integer(value)
            elif name == "#EXT-X-DISCONTINUITY-SEQUENCE":
                disc = _integer(value)
            else:
                attrs = attributes(value)
                if set(attrs) != {"SKIPPED-SEGMENTS"}:
                    raise PlaylistError("unsupported SKIP attributes")
                skip = _integer(attrs["SKIPPED-SEGMENTS"])
        elif line == "#EXT-X-DISCONTINUITY":
            if parts or duration is not None:
                raise PlaylistError("discontinuity inside a segment")
            media_started = True
            offset += 1
        elif line.startswith("#EXT-X-PART:"):
            if duration is not None:
                raise PlaylistError("PART after EXTINF")
            attrs = attributes(line.split(":", 1)[1])
            if not {"URI", "DURATION"} <= attrs.keys() or set(attrs) - {"URI", "DURATION", "INDEPENDENT"}:
                raise PlaylistError("unsupported PART")
            if attrs.get("INDEPENDENT", "NO") not in ("YES", "NO"):
                raise PlaylistError("invalid INDEPENDENT")
            parts.append(Part(_uri(attrs["URI"]), _duration(attrs["DURATION"])))
            media_started = True
        elif line.startswith("#EXTINF:"):
            if duration is not None:
                raise PlaylistError("EXTINF without URI")
            duration = _duration(line.split(":", 1)[1].split(",", 1)[0])
            media_started = True
        elif line == "#EXT-X-ENDLIST":
            if duration is not None:
                raise PlaylistError("unfinished EXTINF")
            ended = True
        elif line.startswith(("#EXT-X-VERSION:", "#EXT-X-TARGETDURATION:")):
            _integer(line.split(":", 1)[1])
        elif line.startswith("#EXT-X-PART-INF:"):
            attrs = attributes(line.split(":", 1)[1])
            if set(attrs) != {"PART-TARGET"}:
                raise PlaylistError("unsupported PART-INF")
            _duration(attrs["PART-TARGET"])
        elif line.startswith("#EXT-X-SERVER-CONTROL:"):
            attrs = attributes(line.split(":", 1)[1])
            if set(attrs) - {"CAN-SKIP-UNTIL", "CAN-BLOCK-RELOAD", "PART-HOLD-BACK", "HOLD-BACK"}:
                raise PlaylistError("unsupported SERVER-CONTROL")
            for key, value in attrs.items():
                if key == "CAN-BLOCK-RELOAD":
                    if value not in ("YES", "NO"):
                        raise PlaylistError("invalid CAN-BLOCK-RELOAD")
                else:
                    _duration(value)
        elif line.startswith("#EXT"):
            raise PlaylistError("unsupported tag: " + line.split(":", 1)[0])
        elif line.startswith("#"):
            continue
        else:
            if duration is None:
                raise PlaylistError("media URI without EXTINF")
            entries.append((offset, tuple(parts), _uri(line), duration))
            parts = []
            duration = None
    if duration is not None:
        raise PlaylistError("unfinished EXTINF")
    if parts:
        entries.append((offset, tuple(parts), None, None))
    if base + skip + len(entries) > 2**63 - 1 or disc + offset > 2**63 - 1:
        raise PlaylistError("position overflow")
    return _Raw(base, disc, skip, tuple(entries), ended)


class Session:
    """Owns one live presentation. Media sequences never reset within a session."""

    def __init__(self):
        self._known = {}
        self.ended = False
        self._end = None
        self._start_seen = None
        self._last_seen = None

    @property
    def declarations(self):
        return tuple(self._known[n] for n in sorted(self._known))

    def _note_window(self, raw):
        last = raw.base + raw.skip + len(raw.entries) - 1
        if last < raw.base:
            return
        self._start_seen = raw.base if self._start_seen is None else min(self._start_seen, raw.base)
        self._last_seen = last if self._last_seen is None else max(self._last_seen, last)

    def missing_ranges(self):
        if self._start_seen is None:
            return ()
        cursor = self._start_seen
        gaps = []
        for seq in sorted(self._known):
            if seq > cursor:
                gaps.append((cursor, seq))
            cursor = max(cursor, seq + 1)
        if cursor <= self._last_seen:
            gaps.append((cursor, self._last_seen + 1))
        return tuple(gaps)

    def accept_playlist(self, text):
        raw = parse(text)
        disc = raw.disc
        if raw.skip:
            # No iteration across a potentially huge absent prefix.
            prefix = [self._known[n] for n in sorted(self._known)
                      if raw.base <= n < raw.base + raw.skip]
            if len(prefix) != raw.skip or any(d.uri is None for d in prefix):
                self._note_window(raw)
                return Update(True, (), self.ended)
            first = prefix[0].position.discontinuity
            if first not in (disc, disc + 1):
                self._note_window(raw)
                return Update(True, (), self.ended)
            previous = first
            for declaration in prefix[1:]:
                current = declaration.position.discontinuity
                if current not in (previous, previous + 1):
                    self._note_window(raw)
                    return Update(True, (), self.ended)
                previous = current
            disc = prefix[-1].position.discontinuity
        candidate = dict(self._known)
        for i, (offset, parts, uri, duration) in enumerate(raw.entries):
            seq = raw.base + raw.skip + i
            position = Position(seq, disc + offset)
            incoming = Declaration(position, parts, uri, duration)
            old = candidate.get(seq)
            if self._end is not None and seq > self._end:
                raise PlaylistError("media added after ENDLIST")
            if old is not None:
                if old.position != position:
                    raise PlaylistError("sequence changed discontinuity")
                for a, b in zip(old.parts, incoming.parts):
                    if a.duration != b.duration:
                        raise PlaylistError("part duration changed")
                merged_parts = incoming.parts + old.parts[len(incoming.parts):]
                if old.duration is not None and duration is not None and old.duration != duration:
                    raise PlaylistError("segment duration changed")
                incoming = replace(incoming, parts=merged_parts,
                                   uri=uri if uri is not None else old.uri,
                                   duration=duration if duration is not None else old.duration)
            if incoming.duration is not None:
                total = sum((p.duration for p in incoming.parts), Fraction(0))
                if total > incoming.duration:
                    raise PlaylistError("parts exceed segment duration")
            candidate[seq] = incoming
        ordered = [candidate[n].position for n in sorted(candidate)]
        if any(a.discontinuity > b.discontinuity for a, b in zip(ordered, ordered[1:])):
            raise PlaylistError("discontinuity moved backwards")
        if raw.ended:
            end = raw.base + raw.skip + len(raw.entries) - 1
            if self._end is not None and end != self._end:
                raise PlaylistError("ENDLIST position changed")
            if candidate and max(candidate) > end:
                raise PlaylistError("ENDLIST precedes known media")
            self._end = end
        self._note_window(raw)
        self._known = candidate
        self.ended = self.ended or raw.ended
        return Update(False, self.declarations, self.ended)
