"""Gap planning over an interval coverage ledger.

Coverage is never inferred from a maximum timestamp.  Every confirmed fetch
writes an interval claim, and gaps are the parts of the requested range that no
*valid* confirmed claim covers:

===================  =========================================================
state                meaning
===================  =========================================================
``confirmed_data``   fetched successfully and records were returned
``confirmed_empty``  fetched successfully and the source said "no data"
``pending``          not fetched yet, retryable failure, unknown outcome, or
                     stale because the source revision moved past the claim
``unrecoverable``    source explicitly said this history cannot be recovered
===================  =========================================================

Claims store the source revision they were fetched at.  A revision mark
(``RevisionMark``) raises the required revision for an interval, so affected
ranges become ``pending`` again while untouched ranges stay confirmed.  A later
successful slice can therefore never mask an earlier gap: coverage is the union
of interval claims, not a watermark.

``process(record)`` is the single-record adapter kept for the fixture pipeline.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable, Sequence

from .timestep import (
    Interval,
    format_instant,
    interval as make_interval,
    merge_intervals,
    parse_instant,
    to_utc,
)

CONFIRMED_DATA = "confirmed_data"
CONFIRMED_EMPTY = "confirmed_empty"
PENDING = "pending"
UNRECOVERABLE = "unrecoverable"

CONFIRMED_STATES = (CONFIRMED_DATA, CONFIRMED_EMPTY)
ALL_STATES = (CONFIRMED_DATA, CONFIRMED_EMPTY, PENDING, UNRECOVERABLE)

_REASON_UNFETCHED = "unfetched"
_REASON_STALE = "stale_revision"
_REASON_UNKNOWN = "unknown_outcome"


# ---------------------------------------------------------------------------
# Claims
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CoverageSlice:
    """One fetch claim about one half-open interval of one source."""

    source: str
    start: datetime
    end: datetime
    state: str = PENDING
    revision: int = 0
    attempts: int = 0
    reason: str | None = None
    updated_at: datetime | None = None
    unit: str = "hour"
    # Which budget reservation paid for this claim.  Recorded explicitly so a
    # retried request reuses its hold instead of reserving again.
    payer: int = 0

    def __post_init__(self) -> None:
        if self.state not in ALL_STATES:
            raise ValueError(f"unknown slice state: {self.state!r}")
        object.__setattr__(self, "start", to_utc(self.start))
        object.__setattr__(self, "end", to_utc(self.end))
        if not self.start < self.end:
            raise ValueError("slice start must be before end")
        if self.updated_at is not None:
            object.__setattr__(self, "updated_at", to_utc(self.updated_at))

    @property
    def interval(self) -> Interval:
        return Interval(self.start, self.end)

    @property
    def slice_id(self) -> str:
        return f"{self.source}@{format_instant(self.start)}/{format_instant(self.end)}"

    @property
    def confirmed(self) -> bool:
        return self.state in CONFIRMED_STATES

    def as_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "start": format_instant(self.start),
            "end": format_instant(self.end),
            "state": self.state,
            "revision": self.revision,
            "attempts": self.attempts,
            "reason": self.reason,
            "unit": self.unit,
            "payer": self.payer,
            "updated_at": format_instant(self.updated_at) if self.updated_at else None,
        }

    @classmethod
    def from_mapping(cls, row: dict[str, Any], tz: str = "UTC") -> "CoverageSlice":
        return cls(
            source=str(row["source"]),
            start=parse_instant(row["start"], tz),
            end=parse_instant(row["end"], tz),
            state=str(row["state"]),
            revision=int(row.get("revision") or 0),
            attempts=int(row.get("attempts") or 0),
            reason=row.get("reason"),
            updated_at=parse_instant(row["updated_at"], tz) if row.get("updated_at") else None,
            unit=str(row.get("unit") or "hour"),
            payer=int(row.get("payer") or 0),
        )


@dataclass(frozen=True)
class RevisionMark:
    """The source observed revision ``revision`` for ``[start, end)``."""

    source: str
    start: datetime
    end: datetime
    revision: int
    reason: str = "source_revision"
    created_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "start", to_utc(self.start))
        object.__setattr__(self, "end", to_utc(self.end))

    @property
    def interval(self) -> Interval:
        return Interval(self.start, self.end)


@dataclass(frozen=True)
class CoverageSegment:
    """A classified elementary interval of the requested range."""

    start: datetime
    end: datetime
    state: str
    revision: int = 0
    reason: str | None = None

    @property
    def interval(self) -> Interval:
        return Interval(self.start, self.end)

    def as_dict(self) -> dict[str, Any]:
        return {
            "start": format_instant(self.start),
            "end": format_instant(self.end),
            "state": self.state,
            "revision": self.revision,
            "reason": self.reason,
        }


@dataclass
class CoverageReport:
    target: Interval
    segments: list[CoverageSegment] = field(default_factory=list)

    def intervals(self, *states: str) -> list[Interval]:
        return merge_intervals(
            seg.interval for seg in self.segments if seg.state in states
        )

    @property
    def covered(self) -> list[Interval]:
        return self.intervals(*CONFIRMED_STATES)

    @property
    def data(self) -> list[Interval]:
        return self.intervals(CONFIRMED_DATA)

    @property
    def empty(self) -> list[Interval]:
        return self.intervals(CONFIRMED_EMPTY)

    @property
    def pending(self) -> list[Interval]:
        return self.intervals(PENDING)

    @property
    def unrecoverable(self) -> list[Interval]:
        return self.intervals(UNRECOVERABLE)

    @property
    def complete(self) -> bool:
        return not self.pending

    def state_at(self, instant: datetime) -> str:
        point = to_utc(instant)
        for seg in self.segments:
            if seg.start <= point < seg.end:
                return seg.state
        raise ValueError(f"{format_instant(point)} is outside the reported target")

    def as_dict(self) -> dict[str, Any]:
        return {
            "target": self.target.as_dict(),
            "segments": [seg.as_dict() for seg in self.segments],
            "confirmed_data": [iv.as_dict() for iv in self.data],
            "confirmed_empty": [iv.as_dict() for iv in self.empty],
            "covered": [iv.as_dict() for iv in self.covered],
            "pending": [iv.as_dict() for iv in self.pending],
            "unrecoverable": [iv.as_dict() for iv in self.unrecoverable],
            "complete": self.complete,
        }


@dataclass
class GapPlan:
    target: Interval
    report: CoverageReport
    work: list[Interval]

    @property
    def done(self) -> bool:
        return not self.work

    def as_dict(self) -> dict[str, Any]:
        return {
            "target": self.target.as_dict(),
            "work": [iv.as_dict() for iv in self.work],
            "coverage": self.report.as_dict(),
        }


# ---------------------------------------------------------------------------
# Coverage resolution
# ---------------------------------------------------------------------------


def _required_revision(
    point_start: datetime,
    point_end: datetime,
    marks: Sequence[RevisionMark],
    base_revision: int,
) -> int:
    required = base_revision
    for mark in marks:
        if mark.start < point_end and point_start < mark.end:
            required = max(required, mark.revision)
    return required


def resolve_coverage(
    target: Interval,
    slices: Iterable[CoverageSlice],
    *,
    marks: Iterable[RevisionMark] = (),
    base_revision: int = 0,
    retry_unrecoverable: bool = False,
) -> CoverageReport:
    """Classify every point of ``target`` from interval claims.

    Confirmed claims only count when their recorded revision satisfies the
    revision required at that point.  Ties prefer data over empty over
    unrecoverable; pending claims never hide a valid confirmed claim.
    """
    claims = [s for s in slices if s.interval.overlaps(target)]
    mark_list = [m for m in marks if m.interval.overlaps(target)]
    boundaries = {target.start, target.end}
    for claim in claims:
        if target.start < claim.start < target.end:
            boundaries.add(claim.start)
        if target.start < claim.end < target.end:
            boundaries.add(claim.end)
    for mark in mark_list:
        if target.start < mark.start < target.end:
            boundaries.add(mark.start)
        if target.start < mark.end < target.end:
            boundaries.add(mark.end)
    points = sorted(boundaries)

    raw: list[CoverageSegment] = []
    for a, b in zip(points, points[1:]):
        required = _required_revision(a, b, mark_list, base_revision)
        covering = [c for c in claims if c.start <= a and b <= c.end]
        state: str | None = None
        revision = 0
        reason: str | None = None

        valid_confirmed = [
            c for c in covering if c.confirmed and c.revision >= required
        ]
        if valid_confirmed:
            best = sorted(
                valid_confirmed,
                key=lambda c: (c.state == CONFIRMED_DATA, c.revision),
                reverse=True,
            )[0]
            state = best.state
            revision = best.revision
            reason = best.reason
        else:
            valid_unrecoverable = [
                c
                for c in covering
                if c.state == UNRECOVERABLE and c.revision >= required
            ]
            if valid_unrecoverable and not retry_unrecoverable:
                state = UNRECOVERABLE
                revision = max(c.revision for c in valid_unrecoverable)
                reason = next(
                    (c.reason for c in valid_unrecoverable if c.reason), None
                )
            else:
                stale = [c for c in covering if c.confirmed and c.revision < required]
                pending_claims = [c for c in covering if c.state == PENDING]
                if stale:
                    state = PENDING
                    revision = required
                    reason = _REASON_STALE
                elif pending_claims:
                    state = PENDING
                    revision = max((c.revision for c in pending_claims), default=required)
                    reason = next(
                        (c.reason for c in pending_claims if c.reason),
                        _REASON_UNKNOWN,
                    )
                else:
                    state = PENDING
                    revision = required
                    reason = _REASON_UNFETCHED

        raw.append(CoverageSegment(a, b, state, revision, reason))

    # Coalesce adjacent elementary segments with identical classification.
    merged: list[CoverageSegment] = []
    for seg in raw:
        if (
            merged
            and merged[-1].end == seg.start
            and merged[-1].state == seg.state
            and merged[-1].reason == seg.reason
            and merged[-1].revision == seg.revision
        ):
            last = merged.pop()
            merged.append(CoverageSegment(last.start, seg.end, seg.state, seg.revision, seg.reason))
        else:
            merged.append(seg)
    return CoverageReport(target=target, segments=merged)


def plan(
    start: datetime | str,
    end: datetime | str,
    slices: Iterable[CoverageSlice] = (),
    *,
    marks: Iterable[RevisionMark] = (),
    base_revision: int = 0,
    retry_unrecoverable: bool = False,
    tz: str = "UTC",
) -> GapPlan:
    """Return the intervals that still need a fetch, plus the classification."""
    target = make_interval(start, end, tz)
    report = resolve_coverage(
        target,
        slices,
        marks=marks,
        base_revision=base_revision,
        retry_unrecoverable=retry_unrecoverable,
    )
    return GapPlan(
        target=target,
        report=report,
        work=merge_intervals(
            [seg.interval for seg in report.segments if seg.state == PENDING]
        ),
    )


# ---------------------------------------------------------------------------
# Fixture pipeline adapter
# ---------------------------------------------------------------------------


def process(record: dict) -> dict:
    """Keep the fixture pipeline contract; canonicalise the record's instant."""
    from .timestep import format_instant

    rec = dict(record)
    tz = str(rec.get("source_tz") or rec.get("tz") or "UTC")
    ts = rec.get("ts") or rec.get("ts_utc")
    if ts:
        try:
            rec.setdefault("ts_utc", format_instant(parse_instant(ts, tz)))
        except (ValueError, TypeError):
            pass
    rec.setdefault("_stage_gapfill", "ok")
    return rec
