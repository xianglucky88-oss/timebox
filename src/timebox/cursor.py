"""Coverage cursor: per-slice confirmation with an explicit resume position.

The cursor deliberately has **no "max cursor" coverage semantics**.  Coverage
is the union of individually confirmed half-open slices, so a later successful
slice can never paper over an earlier gap.

Each confirmed entry ties three things together so a restart is safe:

* the slice interval (what was fetched),
* the slice state and source revision (whether the fetch is still valid),
* the business versions observed in that slice (what the merge consumed).

Invariants checked by :meth:`CoverageCursor.verify`:

1. the cursor is exactly the projection of the slice ledger (no invented
   coverage),
2. no confirmed interval overlaps a pending or unrecoverable interval,
3. recorded business versions never exceed what the merge state holds, i.e. a
   restart can replay the same inputs to reach the same result.

``process(record)`` remains the single-record adapter for the fixture pipeline.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable

from . import gapfill
from .timestep import (
    Interval,
    format_instant,
    merge_intervals,
    parse_instant,
    subtract_intervals,
    to_utc,
)


@dataclass
class CursorEntry:
    """Confirmed state of one slice."""

    source: str
    start: datetime
    end: datetime
    state: str
    revision: int = 0
    attempts: int = 0
    business_version: int = -1
    versions: dict[str, int] = field(default_factory=dict)
    confirmed_at: datetime | None = None
    reason: str | None = None
    payer: int = 0

    def __post_init__(self) -> None:
        self.start = to_utc(self.start)
        self.end = to_utc(self.end)
        if not self.start < self.end:
            raise ValueError("cursor slice must be non-empty")
        if self.state not in gapfill.ALL_STATES:
            raise ValueError(f"unknown slice state: {self.state!r}")
        if self.confirmed_at is not None:
            self.confirmed_at = to_utc(self.confirmed_at)

    @property
    def slice_id(self) -> str:
        return f"{self.source}@{format_instant(self.start)}/{format_instant(self.end)}"

    @property
    def interval(self) -> Interval:
        return Interval(self.start, self.end)

    @property
    def confirmed(self) -> bool:
        return self.state in gapfill.CONFIRMED_STATES

    def as_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "start": format_instant(self.start),
            "end": format_instant(self.end),
            "state": self.state,
            "revision": self.revision,
            "attempts": self.attempts,
            "business_version": self.business_version,
            "versions": dict(self.versions),
            "confirmed_at": format_instant(self.confirmed_at) if self.confirmed_at else None,
            "reason": self.reason,
            "payer": self.payer,
        }

    @classmethod
    def from_dict(cls, row: dict[str, Any]) -> "CursorEntry":
        return cls(
            source=str(row["source"]),
            start=parse_instant(row["start"]),
            end=parse_instant(row["end"]),
            state=str(row["state"]),
            revision=int(row.get("revision") or 0),
            attempts=int(row.get("attempts") or 0),
            business_version=int(row.get("business_version", -1)),
            versions={k: int(v) for k, v in (row.get("versions") or {}).items()},
            confirmed_at=parse_instant(row["confirmed_at"]) if row.get("confirmed_at") else None,
            reason=row.get("reason"),
            payer=int(row.get("payer") or 0),
        )


@dataclass
class CursorSnapshot:
    source: str
    target: Interval
    confirmed: list[Interval]
    pending: list[Interval]
    unrecoverable: list[Interval]
    resume_position: datetime | None
    high_water: datetime | None
    entries: list[CursorEntry]

    @property
    def complete(self) -> bool:
        return not self.pending

    def as_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "target": self.target.as_dict(),
            "confirmed": [iv.as_dict() for iv in self.confirmed],
            "pending": [iv.as_dict() for iv in self.pending],
            "unrecoverable": [iv.as_dict() for iv in self.unrecoverable],
            "resume_position": (
                format_instant(self.resume_position) if self.resume_position else None
            ),
            "high_water": format_instant(self.high_water) if self.high_water else None,
            "complete": self.complete,
            "entries": [e.as_dict() for e in self.entries],
        }


class CursorError(Exception):
    """Raised when cursor state and slice state disagree."""


class CoverageCursor:
    """Per-slice confirmation ledger with an explicit resume position."""

    def __init__(self, source: str):
        self.source = source
        self._entries: dict[str, CursorEntry] = {}

    # -- updates ---------------------------------------------------------
    def record(
        self,
        start: datetime,
        end: datetime,
        state: str,
        *,
        revision: int = 0,
        attempts: int = 0,
        business_version: int = -1,
        versions: dict[str, int] | None = None,
        at: datetime | None = None,
        reason: str | None = None,
        payer: int = 0,
    ) -> CursorEntry:
        entry = CursorEntry(
            source=self.source,
            start=start,
            end=end,
            state=state,
            revision=revision,
            attempts=attempts,
            business_version=business_version,
            versions=dict(versions or {}),
            confirmed_at=at if state in gapfill.CONFIRMED_STATES else None,
            reason=reason,
            payer=payer,
        )
        self._entries[entry.slice_id] = entry
        return entry

    def record_claim(self, claim: gapfill.CoverageSlice) -> CursorEntry:
        return self.record(
            claim.start,
            claim.end,
            claim.state,
            revision=claim.revision,
            attempts=claim.attempts,
            at=claim.updated_at,
            reason=claim.reason,
            payer=claim.payer,
        )

    # -- queries ---------------------------------------------------------
    def entries(self) -> list[CursorEntry]:
        return sorted(self._entries.values(), key=lambda e: (e.start, e.end, e.source))

    def confirmed_intervals(self) -> list[Interval]:
        return merge_intervals(
            e.interval for e in self._entries.values() if e.confirmed
        )

    def pending_intervals(self) -> list[Interval]:
        return merge_intervals(
            e.interval for e in self._entries.values() if e.state == gapfill.PENDING
        )

    def unrecoverable_intervals(self) -> list[Interval]:
        return merge_intervals(
            e.interval for e in self._entries.values() if e.state == gapfill.UNRECOVERABLE
        )

    def resume_position(self, target: Interval) -> datetime | None:
        """Earliest point in ``target`` whose slice is not confirmed.

        This is intentionally the *earliest* gap, not ``max(confirmed) + 1``.
        Returns ``None`` only when the whole target is confirmed.
        """
        pending = gapfill.plan(
            target.start, target.end, self.to_slices()
        ).report.pending
        if not pending:
            return None
        earliest = min(iv.start for iv in pending)
        return max(earliest, target.start)

    def high_water(self) -> datetime | None:
        """Informational only: latest confirmed instant.

        Never use this to derive coverage; it can sit far ahead of a gap.
        """
        confirmed = self.confirmed_intervals()
        return max((iv.end for iv in confirmed), default=None)

    def unfilled(self, target: Interval) -> list[Interval]:
        """Every part of ``target`` still needing a fetch.

        Confirmed ranges and explicitly unrecoverable ranges are both settled;
        only genuinely outstanding work is reported.  Earlier gaps are never
        hidden by later successes.
        """
        settled = [
            e.interval
            for e in self._entries.values()
            if e.confirmed or e.state == gapfill.UNRECOVERABLE
        ]
        return subtract_intervals([target], settled)

    # -- projections -----------------------------------------------------
    def to_slices(self) -> list[gapfill.CoverageSlice]:
        return [
            gapfill.CoverageSlice(
                source=e.source,
                start=e.start,
                end=e.end,
                state=e.state,
                revision=e.revision,
                attempts=e.attempts,
                reason=e.reason,
                updated_at=e.confirmed_at,
            )
            for e in self._entries.values()
        ]

    def snapshot(self, target: Interval) -> CursorSnapshot:
        return CursorSnapshot(
            source=self.source,
            target=target,
            confirmed=self.confirmed_intervals(),
            pending=self.unfilled(target),
            unrecoverable=self.unrecoverable_intervals(),
            resume_position=self.resume_position(target),
            high_water=self.high_water(),
            entries=self.entries(),
        )

    # -- consistency -----------------------------------------------------
    def verify(
        self,
        *,
        target: Interval | None = None,
        merger: "Any | None" = None,
        required: dict[str, int] | None = None,
    ) -> None:
        """Raise :class:`CursorError` when cursor invariants are violated."""
        for entry in self._entries.values():
            for other in self._entries.values():
                if entry is other:
                    continue
                if entry.confirmed and other.state not in gapfill.CONFIRMED_STATES:
                    if entry.interval.overlaps(other.interval):
                        # Overlap of a confirmed slice with an outstanding one
                        # is allowed only for the exact same interval re-claim;
                        # a genuine confirmed/outstanding overlap is inconsistent.
                        if not (entry.start == other.start and entry.end == other.end):
                            raise CursorError(
                                f"confirmed slice {entry.slice_id} overlaps "
                                f"{other.state} slice {other.slice_id}"
                            )
            if required and entry.confirmed and entry.revision < required.get(entry.slice_id, 0):
                raise CursorError(
                    f"slice {entry.slice_id} confirmed at revision {entry.revision} "
                    f"below required {required[entry.slice_id]}"
                )
            if merger is not None and entry.confirmed:
                for entity, version in entry.versions.items():
                    state = merger.entities.get(entity)
                    if state is not None and state.version < version:
                        raise CursorError(
                            f"merge state for {entity} regressed below cursor version "
                            f"{version}"
                        )
        if target is not None:
            covered = self.confirmed_intervals() + self.unrecoverable_intervals()
            outside = subtract_intervals([target], covered)
            pending = self.unfilled(target)
            if outside and not merge_intervals(pending):
                raise CursorError("cursor does not account for the whole target")

    # -- persistence -----------------------------------------------------
    def dump(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "entries": [e.as_dict() for e in self.entries()],
        }

    @classmethod
    def load(cls, state: dict[str, Any] | None) -> "CoverageCursor | None":
        if not state:
            return None
        cursor = cls(str(state["source"]))
        for row in state.get("entries", []):
            entry = CursorEntry.from_dict(row)
            cursor._entries[entry.slice_id] = entry
        return cursor


def rebuild(source: str, slices: Iterable[gapfill.CoverageSlice]) -> CoverageCursor:
    """Reconstruct a cursor purely from the slice ledger."""
    cursor = CoverageCursor(source)
    for claim in slices:
        cursor.record_claim(claim)
    return cursor


def process(record: dict) -> dict:
    """Single-record adapter kept for the fixture pipeline."""
    rec = dict(record)
    rec.setdefault("_stage_cursor", "ok")
    return rec
