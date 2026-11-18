"""Time slicing with one canonical convention across the whole project.

Conventions (kept deliberately in one place so every stage agrees):

* An instant is a timezone-aware :class:`datetime` normalised to UTC with
  microsecond resolution.  Canonical text form is
  ``YYYY-MM-DDTHH:MM:SS.ffffffZ`` and sorts lexicographically.
* An interval is **half-open** ``[start, end)``.  Adjacent intervals tile
  exactly: no instant is counted twice and none is skipped.
* Slicing happens in the *source* IANA timezone.  Day boundaries are local
  midnights, hour boundaries are local wall-clock hours.  Every local wall
  time is resolved to *all* matching UTC instants (DST fold) and nonexistent
  wall times are skipped (DST gap).  Consequently the union of the slices is
  the original ``[start, end)`` regardless of whether it was split by hour or
  by day.
* ``precision`` is the coarsest unit a caller accepts for range bounds.  It is
  applied consistently (start rounded down, end rounded up) before slicing.

``process(record)`` remains a pure, single-record adapter for the fixture
pipeline; the real slicing API is :func:`slice_range` / :func:`normalize_range`.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta, timezone
from typing import Iterable, Sequence
from zoneinfo import ZoneInfo

UTC = timezone.utc

PRECISIONS = ("microsecond", "millisecond", "second", "minute", "hour", "day")
UNITS = ("hour", "day")

_PRECISION_RANK = {p: i for i, p in enumerate(PRECISIONS)}


# ---------------------------------------------------------------------------
# Instants
# ---------------------------------------------------------------------------


def to_utc(value: datetime) -> datetime:
    """Return ``value`` as an aware UTC datetime."""
    if value.tzinfo is None:
        raise ValueError("naive datetime is not an instant; attach a timezone first")
    return value.astimezone(UTC)


def parse_instant(value: datetime | str, tz: str = "UTC") -> datetime:
    """Parse an ISO-8601 instant; naive text is interpreted in ``tz``."""
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            raise ValueError("empty instant")
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo(tz))
    return dt.astimezone(UTC)


def format_instant(value: datetime) -> str:
    """Canonical UTC text form used for persistence and comparisons."""
    return to_utc(value).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def local_wall(instant: datetime, tz: str) -> datetime:
    """Wall-clock datetime (naive, fold retained) for an instant in ``tz``."""
    local = to_utc(instant).astimezone(ZoneInfo(tz))
    return local.replace(tzinfo=None)


def wall_candidates(wall: datetime, tz: str) -> list[datetime]:
    """All UTC instants matching a local wall time.

    Returns two instants for an ambiguous (fold) wall time, none for a
    nonexistent (gap) wall time.
    """
    tzobj = ZoneInfo(tz)
    found: set[datetime] = set()
    for fold in (0, 1):
        local = wall.replace(tzinfo=tzobj, fold=fold)
        utc = local.astimezone(UTC)
        if utc.astimezone(tzobj).replace(tzinfo=None) == wall:
            found.add(utc)
    return sorted(found)


def _first_valid_after(wall: datetime, tz: str) -> list[datetime]:
    """First existing wall time at/after ``wall`` (used for DST midnight gaps)."""
    for minutes in range(0, 24 * 60):
        found = wall_candidates(wall + timedelta(minutes=minutes), tz)
        if found:
            return found[:1]
    return []


def resolve_wall(wall: datetime, tz: str) -> list[datetime]:
    """Resolve a local wall time, tolerating a gap by moving to the first
    valid instant.  Ambiguous times still return both instants."""
    return wall_candidates(wall, tz) or _first_valid_after(wall, tz)


# ---------------------------------------------------------------------------
# Precision
# ---------------------------------------------------------------------------


def _truncate_wall(wall: datetime, precision: str) -> datetime:
    if precision == "microsecond":
        return wall
    if precision == "millisecond":
        return wall.replace(microsecond=wall.microsecond // 1000 * 1000)
    if precision == "second":
        return wall.replace(microsecond=0)
    if precision == "minute":
        return wall.replace(second=0, microsecond=0)
    if precision == "hour":
        return wall.replace(minute=0, second=0, microsecond=0)
    if precision == "day":
        return datetime.combine(wall.date(), dtime.min)
    raise ValueError(f"unknown precision: {precision!r}")


def truncate(instant: datetime, precision: str = "microsecond", tz: str = "UTC") -> datetime:
    """Round an instant down to ``precision`` in the source timezone."""
    if precision not in _PRECISION_RANK:
        raise ValueError(f"unknown precision: {precision!r}")
    if precision == "microsecond":
        return to_utc(instant)
    wall = _truncate_wall(local_wall(instant, tz), precision)
    candidates = resolve_wall(wall, tz)
    if not candidates:
        return to_utc(instant)
    # Pick the latest candidate that is not after the original instant.
    floor = [c for c in candidates if c <= to_utc(instant)]
    return floor[-1] if floor else candidates[0]


def ceil(instant: datetime, precision: str = "microsecond", tz: str = "UTC") -> datetime:
    """Round an instant up to ``precision`` in the source timezone."""
    floored = truncate(instant, precision, tz)
    if floored == to_utc(instant):
        return floored
    if precision == "microsecond":
        return to_utc(instant)
    if precision == "millisecond":
        return floored + timedelta(microseconds=1000)
    if precision == "second":
        return floored + timedelta(seconds=1)
    if precision == "minute":
        return floored + timedelta(minutes=1)
    if precision == "hour":
        wall = local_wall(floored, tz) + timedelta(hours=1)
    elif precision == "day":
        wall = datetime.combine(local_wall(floored, tz).date() + timedelta(days=1), dtime.min)
    else:  # pragma: no cover - guarded above
        raise ValueError(f"unknown precision: {precision!r}")
    candidates = [c for c in resolve_wall(wall, tz) if c > to_utc(instant)]
    return candidates[0] if candidates else to_utc(instant)


def normalize_range(
    start: datetime | str,
    end: datetime | str,
    *,
    precision: str = "microsecond",
    tz: str = "UTC",
) -> tuple[datetime, datetime]:
    """Normalise range bounds to the project convention.

    The start is rounded down and the end rounded up to ``precision`` so that
    both an hour split and a day split see exactly the same bounds.
    """
    s = truncate(parse_instant(start, tz), precision, tz)
    e = ceil(parse_instant(end, tz), precision, tz)
    if e <= s:
        raise ValueError("range end must be after range start")
    return s, e


# ---------------------------------------------------------------------------
# Intervals
# ---------------------------------------------------------------------------


@dataclass(frozen=True, order=True)
class Interval:
    """Half-open UTC interval ``[start, end)``."""

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "start", to_utc(self.start))
        object.__setattr__(self, "end", to_utc(self.end))
        if not self.start < self.end:
            raise ValueError(f"invalid interval [{self.start!r}, {self.end!r})")

    @property
    def duration(self) -> timedelta:
        return self.end - self.start

    def contains(self, instant: datetime) -> bool:
        return self.start <= to_utc(instant) < self.end

    def __contains__(self, instant: datetime) -> bool:
        # Allows ``instant in interval`` with the same half-open semantics.
        return isinstance(instant, datetime) and self.contains(instant)

    def overlaps(self, other: "Interval") -> bool:
        return self.start < other.end and other.start < self.end

    def intersection(self, other: "Interval") -> "Interval | None":
        s = max(self.start, other.start)
        e = min(self.end, other.end)
        return Interval(s, e) if s < e else None

    def clip(self, start: datetime, end: datetime) -> "Interval | None":
        return self.intersection(Interval(start, end))

    def as_dict(self) -> dict[str, str]:
        return {"start": format_instant(self.start), "end": format_instant(self.end)}

    def __str__(self) -> str:  # pragma: no cover - debugging aid
        return f"[{format_instant(self.start)}, {format_instant(self.end)})"


def interval(start: datetime | str, end: datetime | str, tz: str = "UTC") -> Interval:
    return Interval(parse_instant(start, tz), parse_instant(end, tz))


def merge_intervals(intervals: Iterable[Interval]) -> list[Interval]:
    """Union of intervals.  Touching intervals are coalesced."""
    ordered = sorted(intervals)
    out: list[Interval] = []
    for iv in ordered:
        if out and iv.start <= out[-1].end:
            if iv.end > out[-1].end:
                out[-1] = Interval(out[-1].start, iv.end)
        else:
            out.append(iv)
    return out


def intersect_intervals(a: Iterable[Interval], b: Iterable[Interval]) -> list[Interval]:
    left, right = merge_intervals(a), merge_intervals(b)
    out: list[Interval] = []
    i = j = 0
    while i < len(left) and j < len(right):
        hit = left[i].intersection(right[j])
        if hit:
            out.append(hit)
        if left[i].end <= right[j].end:
            i += 1
        else:
            j += 1
    return merge_intervals(out)


def subtract_intervals(base: Iterable[Interval], holes: Iterable[Interval]) -> list[Interval]:
    """``base \\ holes`` as a sorted list of half-open intervals."""
    remaining = merge_intervals(base)
    holes = merge_intervals(holes)
    out: list[Interval] = []
    for iv in remaining:
        cursor = iv.start
        for hole in holes:
            if hole.end <= cursor:
                continue
            if hole.start >= iv.end:
                break
            if hole.start > cursor:
                out.append(Interval(cursor, min(hole.start, iv.end)))
            cursor = max(cursor, hole.end)
            if cursor >= iv.end:
                break
        if cursor < iv.end:
            out.append(Interval(cursor, iv.end))
    return merge_intervals(out)


def total_seconds(intervals: Iterable[Interval]) -> float:
    return sum((iv.duration.total_seconds() for iv in merge_intervals(intervals)), 0.0)


def same_coverage(a: Iterable[Interval], b: Iterable[Interval]) -> bool:
    return merge_intervals(a) == merge_intervals(b)


# ---------------------------------------------------------------------------
# Slicing
# ---------------------------------------------------------------------------


def _boundaries(start: datetime, end: datetime, unit: str, tz: str) -> list[datetime]:
    if unit not in UNITS:
        raise ValueError(f"unknown slicing unit: {unit!r}")
    tzobj = ZoneInfo(tz)
    bounds: set[datetime] = {start, end}
    first_day: date = start.astimezone(tzobj).date()
    last_day: date = (end - timedelta(microseconds=1)).astimezone(tzobj).date()
    day = first_day
    while day <= last_day:
        if unit == "day":
            walls = [datetime.combine(day, dtime.min)]
        else:
            walls = [datetime.combine(day, dtime(hour=h)) for h in range(24)]
        for wall in walls:
            for boundary in resolve_wall(wall, tz):
                if start < boundary < end:
                    bounds.add(boundary)
        day += timedelta(days=1)
    return sorted(bounds)


def slice_range(
    start: datetime | str,
    end: datetime | str,
    *,
    unit: str = "hour",
    tz: str = "UTC",
    precision: str = "microsecond",
) -> list[Interval]:
    """Split ``[start, end)`` into half-open slices of ``unit`` in ``tz``.

    The union of the returned intervals is always exactly the normalized
    range, for both ``unit="hour"`` and ``unit="day"``.
    """
    if unit not in UNITS:
        raise ValueError(f"unknown slicing unit: {unit!r}")
    if precision not in _PRECISION_RANK:
        raise ValueError(f"unknown precision: {precision!r}")
    if _PRECISION_RANK[precision] > _PRECISION_RANK[unit]:
        raise ValueError(f"precision {precision!r} is coarser than slicing unit {unit!r}")
    s, e = normalize_range(start, end, precision=precision, tz=tz)
    points = _boundaries(s, e, unit, tz)
    return [Interval(a, b) for a, b in zip(points, points[1:]) if a < b]


def validate_slices(slices: Sequence[Interval], start: datetime, end: datetime) -> None:
    """Raise if ``slices`` do not form an exact partition of ``[start, end)``."""
    s, e = to_utc(start), to_utc(end)
    if not slices:
        raise ValueError("no slices returned")
    if slices[0].start != s or slices[-1].end != e:
        raise ValueError("slices do not cover the requested range bounds")
    for left, right in zip(slices, slices[1:]):
        if left.end != right.start:
            raise ValueError(f"slice gap/overlap at {format_instant(left.end)}")


# ---------------------------------------------------------------------------
# Record adapter kept for the fixture pipeline
# ---------------------------------------------------------------------------


def process(record: dict) -> dict:
    """Attach canonical timestamp information without changing other fields."""
    rec = dict(record)
    tz = str(rec.get("source_tz") or rec.get("tz") or "UTC")
    ts = rec.get("ts")
    if ts:
        try:
            rec.setdefault("ts_utc", format_instant(parse_instant(ts, tz)))
        except (ValueError, TypeError):
            # Malformed timestamps are handled by the validating ingestion
            # path; the fixture pipeline stays permissive.
            pass
    rec.setdefault("_stage_timestep", "ok")
    return rec
