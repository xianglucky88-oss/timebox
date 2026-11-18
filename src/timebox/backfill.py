"""Backfill orchestration: gap plan -> slices -> fetch -> merge -> cursor.

The engine composes the focused modules and owns the *order* of operations
only; every business rule lives in ``gapfill`` (coverage), ``timestep``
(interval conventions), ``budget`` (quotas), ``merge`` (versions/evidence) and
``cursor`` (confirmation).  Restart safety follows from that composition:

* A slice is fetched only when :mod:`gapfill` classifies it as ``pending`` for
  the required revision, so confirmed or explicitly-empty ranges are skipped
  and revision-affected ranges are re-fetched.
* The cursor is a projection of the slice ledger, so it cannot claim coverage
  the ledger does not have.
* Every budget operation is keyed by slice + attempt.  A retried request reuses
  its reservation, and an unknown outcome keeps the hold instead of charging
  twice.  Holds expire so backfill cannot block realtime forever.
* Old inputs are retained: the merge history keeps every observation, and the
  engine state dump is self-contained for restart.

Fetches are injected through :class:`Source`, which keeps the engine offline
testable and free of transport details.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable, Protocol

from . import budget as budget_mod
from . import cursor as cursor_mod
from . import gapfill
from . import merge as merge_mod
from . import timestep
from .timestep import Interval, format_instant, parse_instant


class FetchOutcome:
    DATA = "data"
    EMPTY = "empty"
    UNRECOVERABLE = "unrecoverable"
    RETRYABLE = "retryable"
    UNKNOWN = "unknown"


OUTCOME_TO_STATE = {
    FetchOutcome.DATA: gapfill.CONFIRMED_DATA,
    FetchOutcome.EMPTY: gapfill.CONFIRMED_EMPTY,
    FetchOutcome.UNRECOVERABLE: gapfill.UNRECOVERABLE,
    FetchOutcome.RETRYABLE: gapfill.PENDING,
    FetchOutcome.UNKNOWN: gapfill.PENDING,
}


@dataclass
class SourceResponse:
    """What a source answered for one slice.

    ``cost`` is the actual budget consumed (defaults to one unit).  A source
    that answers at all has consumed its request even when it returns no data.
    """

    outcome: str
    observations: list[merge_mod.Observation] = field(default_factory=list)
    revision: int = 0
    cost: int = 1
    detail: str = ""

    def __post_init__(self) -> None:
        if self.outcome not in {
            FetchOutcome.DATA,
            FetchOutcome.EMPTY,
            FetchOutcome.UNRECOVERABLE,
            FetchOutcome.RETRYABLE,
            FetchOutcome.UNKNOWN,
        }:
            raise ValueError(f"unknown fetch outcome: {self.outcome!r}")


class Source(Protocol):
    """Minimal fetch interface; implementations must be side-effect free."""

    name: str

    def fetch(
        self, interval: Interval, *, revision: int, attempt: int
    ) -> SourceResponse:  # pragma: no cover - protocol
        ...


class StaticSource:
    """Offline source backed by a callable or a mapping of intervals."""

    def __init__(
        self,
        name: str,
        responder: "Any",
        *,
        revision: int = 0,
        unrecoverable: Iterable[Interval] = (),
    ):
        self.name = name
        self.responder = responder
        self.revision = revision
        self.unrecoverable = list(unrecoverable)

    def fetch(self, interval: Interval, *, revision: int, attempt: int) -> SourceResponse:
        for hole in self.unrecoverable:
            if hole.overlaps(interval):
                return SourceResponse(
                    FetchOutcome.UNRECOVERABLE,
                    revision=self.revision,
                    detail="source reports this history is not recoverable",
                )
        if callable(self.responder):
            result = self.responder(interval, revision=revision, attempt=attempt)
        else:
            result = self.responder.get(interval, [])
        if isinstance(result, SourceResponse):
            return result
        records = list(result or [])
        observations = [
            merge_mod.Observation(
                entity=str(rec.get("id") or rec.get("key")),
                version=int(rec.get("version", 1)),
                payload=dict(rec),
                source=str(rec.get("src") or self.name),
                origin="backfill",
                revision=self.revision,
                event_ts=_event_ts(rec),
            )
            for rec in records
        ]
        return SourceResponse(
            FetchOutcome.DATA if observations else FetchOutcome.EMPTY,
            observations=observations,
            revision=self.revision,
        )


def _event_ts(rec: dict) -> datetime | None:
    value = rec.get("event_ts") or rec.get("ts")
    if not value:
        return None
    try:
        return parse_instant(value, str(rec.get("source_tz") or "UTC"))
    except (ValueError, TypeError):
        return None


def _slice_key(source: str, iv: Interval) -> str:
    """Canonical slice id; must match :attr:`gapfill.CoverageSlice.slice_id`."""
    return f"{source}@{format_instant(iv.start)}/{format_instant(iv.end)}"


@dataclass
class SliceRun:
    """Auditable outcome of one attempted slice."""

    interval: Interval
    state: str
    attempts: int
    revision: int
    observation_count: int
    action: str
    op_id: str | None
    detail: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "interval": self.interval.as_dict(),
            "state": self.state,
            "attempts": self.attempts,
            "revision": self.revision,
            "observations": self.observation_count,
            "action": self.action,
            "op_id": self.op_id,
            "detail": self.detail,
        }


@dataclass
class BackfillRun:
    """Result of one :meth:`BackfillEngine.run` call."""

    target: Interval
    slices: list[SliceRun] = field(default_factory=list)
    skipped: list[Interval] = field(default_factory=list)
    budget_exhausted: bool = False
    stopped_early: bool = False

    @property
    def done(self) -> bool:
        return not self.budget_exhausted and not self.stopped_early

    def as_dict(self) -> dict[str, Any]:
        return {
            "target": self.target.as_dict(),
            "slices": [s.as_dict() for s in self.slices],
            "skipped": [iv.as_dict() for iv in self.skipped],
            "budget_exhausted": self.budget_exhausted,
            "stopped_early": self.stopped_early,
            "done": self.done,
        }


class BackfillEngine:
    """Composes gap planning, slicing, fetching, merging and cursor state."""

    def __init__(
        self,
        source: Source,
        *,
        source_tz: str = "UTC",
        precision: str = "microsecond",
        unit: str = "hour",
        budget: budget_mod.BudgetLedger | None = None,
        merger: merge_mod.Merger | None = None,
        cursor: cursor_mod.CoverageCursor | None = None,
        base_revision: int = 0,
    ):
        if unit not in timestep.UNITS:
            raise ValueError(f"unknown slicing unit: {unit!r}")
        self.source = source
        self.source_tz = source_tz
        self.precision = precision
        self.unit = unit
        self.budget = budget or budget_mod.BudgetLedger(1000)
        self.merger = merger or merge_mod.Merger()
        self.cursor = cursor or cursor_mod.CoverageCursor(source.name)
        self.base_revision = base_revision
        self.slices: dict[str, gapfill.CoverageSlice] = {}
        self.marks: list[gapfill.RevisionMark] = []
        for entry in self.cursor.entries():
            self.slices[entry.slice_id] = gapfill.CoverageSlice(
                source=entry.source,
                start=entry.start,
                end=entry.end,
                state=entry.state,
                revision=entry.revision,
                attempts=entry.attempts,
                reason=entry.reason,
                updated_at=entry.confirmed_at,
                unit=self.unit,
            )

    # -- planning --------------------------------------------------------
    def mark_revision(
        self,
        start: datetime | str,
        end: datetime | str,
        revision: int,
        *,
        reason: str = "source_revision",
    ) -> gapfill.RevisionMark:
        """Declare that ``[start, end)`` now requires ``revision``."""
        iv = timestep.interval(start, end, self.source_tz)
        mark = gapfill.RevisionMark(
            source=self.source.name,
            start=iv.start,
            end=iv.end,
            revision=revision,
            reason=reason,
        )
        self.marks.append(mark)
        return mark

    def plan(
        self,
        start: datetime | str,
        end: datetime | str,
        *,
        retry_unrecoverable: bool = False,
    ) -> gapfill.GapPlan:
        target = timestep.interval(
            *timestep.normalize_range(
                start, end, precision=self.precision, tz=self.source_tz
            )
        )
        return gapfill.plan(
            target.start,
            target.end,
            self.slices.values(),
            marks=self.marks,
            base_revision=self.base_revision,
            retry_unrecoverable=retry_unrecoverable,
        )

    def work_items(
        self,
        start: datetime | str,
        end: datetime | str,
        *,
        unit: str | None = None,
        retry_unrecoverable: bool = False,
    ) -> list[Interval]:
        """Pending intervals split into fetch-sized slices.

        ``unit`` controls how *newly planned* space is sliced.  A range that
        already carries an outstanding claim keeps that claim's extent on
        retry: the extent is part of the claim's identity (and of its budget
        reservation), so coarsening or refining it mid-retry would either
        duplicate a hold or lose the attempt count.  Coverage is unaffected
        either way, because hour and day slices of the same range are equal.
        """
        chosen = unit or self.unit
        plan = self.plan(start, end, retry_unrecoverable=retry_unrecoverable)
        return self._items_for(plan, chosen)

    def _items_for(self, plan: gapfill.GapPlan, chosen: str) -> list[Interval]:
        # Only outstanding claims are reusable as primitives; confirmed and
        # unrecoverable claims are never fetched again by this path.
        primitives = {
            claim.interval
            for claim in self.slices.values()
            if claim.state == gapfill.PENDING
        }
        items: list[Interval] = []
        for iv in plan.work:
            if iv in primitives:
                items.append(iv)
                continue
            # Reuse any recorded pending primitive that lies inside this gap.
            nested = sorted(
                (p for p in primitives if iv.start <= p.start and p.end <= iv.end),
                key=lambda p: p.start,
            )
            cursor = iv.start
            for piece in nested:
                if piece.start > cursor:
                    items.extend(
                        timestep.slice_range(
                            cursor,
                            piece.start,
                            unit=chosen,
                            tz=self.source_tz,
                            precision=self.precision,
                        )
                    )
                items.append(piece)
                cursor = piece.end
            if cursor < iv.end:
                items.extend(
                    timestep.slice_range(
                        cursor,
                        iv.end,
                        unit=chosen,
                        tz=self.source_tz,
                        precision=self.precision,
                    )
                )
        return items

    # -- execution -------------------------------------------------------
    def run(
        self,
        start: datetime | str,
        end: datetime | str,
        *,
        unit: str | None = None,
        retry_unrecoverable: bool = False,
        max_slices: int | None = None,
        at: datetime | None = None,
    ) -> BackfillRun:
        """Fetch pending slices until done, out of budget, or ``max_slices``."""
        chosen = unit or self.unit
        plan = self.plan(start, end, retry_unrecoverable=retry_unrecoverable)
        run = BackfillRun(target=plan.target)
        covered = [seg.interval for seg in plan.report.segments if seg.state != gapfill.PENDING]
        run.skipped = covered

        items = self._items_for(plan, chosen)

        processed = 0
        for iv in items:
            if max_slices is not None and processed >= max_slices:
                run.stopped_early = True
                break
            processed += 1
            result, exhausted = self._fetch_slice(
                iv, unit=chosen, at=at
            )
            run.slices.append(result)
            if exhausted:
                run.budget_exhausted = True
                break

        self._sync_cursor()
        return run

    def _fetch_slice(
        self, iv: Interval, *, unit: str, at: datetime | None
    ) -> tuple[SliceRun, bool]:
        key = _slice_key(self.source.name, iv)
        previous = self.slices.get(key)
        required = self._required_revision(iv)
        report_attempt = (previous.attempts if previous else 0) + 1

        # Reuse the previous payer while the previous outcome is still unknown:
        # that request may have been served and its hold is still outstanding.
        if previous is not None and previous.reason == "unknown_outcome":
            payer_attempt = previous.payer
            op_id = f"bf:{key}:a{payer_attempt}"
            if self.budget.get(op_id) is None:
                # The hold expired (or was never persisted): take a new one so
                # the charge is still accounted for.
                payer_attempt = report_attempt
                op_id = f"bf:{key}:a{payer_attempt}"
                if not self._reserve(op_id, at, key):
                    return self._deferred(iv, previous, required, at), True
        else:
            payer_attempt = report_attempt
            op_id = f"bf:{key}:a{payer_attempt}"
            if not self._reserve(op_id, at, key):
                return self._deferred(iv, previous, required, at), True

        try:
            response = self.source.fetch(iv, revision=required, attempt=report_attempt)
        except Exception as exc:  # transport blew up mid-request
            response = SourceResponse(
                FetchOutcome.UNKNOWN,
                revision=required,
                detail=f"transport error: {exc.__class__.__name__}",
            )

        if response.outcome == FetchOutcome.UNKNOWN:
            self.budget.mark_unknown(
                op_id, at=at, note=response.detail or "unknown outcome"
            )
            state = gapfill.PENDING
            reason = "unknown_outcome"
            action = "hold_kept"
        elif response.outcome == FetchOutcome.RETRYABLE:
            self.budget.refund(op_id, reason="retryable_error", at=at)
            state = gapfill.PENDING
            reason = "retryable_error"
            action = "refunded"
        else:
            self.budget.settle(
                op_id,
                max(0, int(response.cost if response.cost is not None else 1)),
                at=at,
                note=response.detail,
            )
            state = OUTCOME_TO_STATE[response.outcome]
            reason = response.outcome
            action = "settled"

        versions: dict[str, int] = {}
        applied_actions: list[str] = []
        if response.outcome == FetchOutcome.DATA:
            for obs in response.observations:
                result = self.merger.apply(obs)
                versions[obs.entity] = max(versions.get(obs.entity, -1), obs.version)
                applied_actions.append(result.action)

        claim = gapfill.CoverageSlice(
            source=self.source.name,
            start=iv.start,
            end=iv.end,
            state=state,
            revision=required,
            attempts=report_attempt,
            reason=reason,
            updated_at=at,
            unit=unit,
            payer=payer_attempt,
        )
        self.slices[key] = claim
        business_version = max(versions.values(), default=-1)
        self.cursor.record(
            iv.start,
            iv.end,
            state,
            revision=required,
            attempts=report_attempt,
            business_version=business_version,
            versions=versions,
            at=at,
            reason=reason,
            payer=payer_attempt,
        )

        detail = response.detail
        if applied_actions:
            detail = (detail + " " if detail else "") + ",".join(applied_actions)
        return (
            SliceRun(
                interval=iv,
                state=state,
                attempts=report_attempt,
                revision=required,
                observation_count=len(response.observations),
                action=action,
                op_id=op_id,
                detail=detail,
            ),
            False,
        )

    def _reserve(self, op_id: str, at: datetime | None, note: str) -> bool:
        try:
            self.budget.reserve(op_id, 1, lane="backfill", at=at, note=note)
            return True
        except budget_mod.BudgetExceeded:
            return False

    def _deferred(
        self,
        iv: Interval,
        previous: gapfill.CoverageSlice | None,
        required: int,
        at: datetime | None,
    ) -> SliceRun:
        return SliceRun(
            interval=iv,
            state=gapfill.PENDING,
            attempts=previous.attempts if previous else 0,
            revision=required,
            observation_count=0,
            action="deferred",
            op_id=None,
            detail="budget_exhausted",
        )

    def _required_revision(self, iv: Interval) -> int:
        required = self.base_revision
        for mark in self.marks:
            if mark.start < iv.end and iv.start < mark.end:
                required = max(required, mark.revision)
        return required

    def _sync_cursor(self) -> None:
        for claim in self.slices.values():
            existing = self.cursor._entries.get(claim.slice_id)
            if existing is None:
                self.cursor.record_claim(claim)

    # -- realtime --------------------------------------------------------
    def ingest_realtime(
        self,
        observations: Iterable[merge_mod.Observation],
        *,
        op_id: str,
        cost: int = 1,
        at: datetime | None = None,
        reserve: bool = True,
    ) -> list[merge_mod.MergeResult]:
        """Apply realtime observations against the same budget and merge state.

        The realtime lane draws from full capacity, so it is never starved by
        backfill reservations.
        """
        observations = list(observations)
        if reserve:
            self.budget.reserve(op_id, cost, lane="realtime", at=at, note="realtime")
        results = []
        for obs in observations:
            if obs.origin == "backfill":
                obs = merge_mod.Observation(
                    entity=obs.entity,
                    version=obs.version,
                    payload=obs.payload,
                    source=obs.source,
                    origin="realtime",
                    revision=obs.revision,
                    observed_at=obs.observed_at,
                    event_ts=obs.event_ts,
                    deleted=obs.deleted,
                )
            results.append(self.merger.apply(obs))
        if reserve:
            self.budget.settle(op_id, cost, at=at, note="realtime")
        return results

    # -- reporting -------------------------------------------------------
    def status(self, start: datetime | str, end: datetime | str) -> dict[str, Any]:
        plan = self.plan(start, end)
        snapshot = self.cursor.snapshot(plan.target)
        results = {
            entity: state.as_dict()
            for entity, state in sorted(self.merger.entities.items())
        }
        return {
            "source": self.source.name,
            "target": plan.target.as_dict(),
            "coverage": plan.report.as_dict(),
            "cursor": snapshot.as_dict(),
            "unfilled": [iv.as_dict() for iv in snapshot.pending],
            "resume_position": (
                format_instant(snapshot.resume_position) if snapshot.resume_position else None
            ),
            "results": results,
            "review_queue": self.merger.review_queue(),
            "budget": self.budget.snapshot(),
        }

    def explain_budget(self, op_id: str) -> dict[str, Any] | None:
        return self.budget.explain(op_id)

    def verify(self, start: datetime | str, end: datetime | str) -> None:
        plan = self.plan(start, end)
        self.cursor.verify(target=plan.target, merger=self.merger)
        # Cursor projection must equal the slice ledger projection.
        ledger_states = {}
        for claim in self.slices.values():
            ledger_states.setdefault(claim.slice_id, claim.state)
        cursor_states = {
            entry.slice_id: entry.state for entry in self.cursor.entries()
        }
        for slice_id, state in cursor_states.items():
            if slice_id in ledger_states and ledger_states[slice_id] != state:
                raise cursor_mod.CursorError(
                    f"cursor state {state} disagrees with ledger {ledger_states[slice_id]} "
                    f"for {slice_id}"
                )
        for slice_id in ledger_states:
            if slice_id not in cursor_states:
                raise cursor_mod.CursorError(f"ledger slice {slice_id} missing from cursor")

    # -- persistence -----------------------------------------------------
    def dump(self) -> dict[str, Any]:
        return {
            "source": self.source.name,
            "source_tz": self.source_tz,
            "precision": self.precision,
            "unit": self.unit,
            "base_revision": self.base_revision,
            "slices": [s.as_dict() for s in self.slices.values()],
            "marks": [
                {
                    "source": m.source,
                    "start": format_instant(m.start),
                    "end": format_instant(m.end),
                    "revision": m.revision,
                    "reason": m.reason,
                }
                for m in self.marks
            ],
            "budget": self.budget.dump(),
            "merge": self.merger.dump(),
            "cursor": self.cursor.dump(),
        }

    @classmethod
    def load(cls, state: dict[str, Any], source: Source) -> "BackfillEngine":
        budget = budget_mod.BudgetLedger.load(state.get("budget"))
        merger = merge_mod.Merger.load(state.get("merge"))
        cursor = cursor_mod.CoverageCursor.load(state.get("cursor"))
        engine = cls(
            source,
            source_tz=state.get("source_tz", "UTC"),
            precision=state.get("precision", "microsecond"),
            unit=state.get("unit", "hour"),
            budget=budget,
            merger=merger,
            cursor=cursor,
            base_revision=int(state.get("base_revision", 0)),
        )
        engine.slices = {
            row["source"] + "@" + row["start"] + "/" + row["end"]: gapfill.CoverageSlice.from_mapping(
                row, tz=engine.source_tz
            )
            for row in state.get("slices", [])
        }
        engine.marks = [
            gapfill.RevisionMark(
                source=row["source"],
                start=parse_instant(row["start"], engine.source_tz),
                end=parse_instant(row["end"], engine.source_tz),
                revision=int(row["revision"]),
                reason=row.get("reason", "source_revision"),
            )
            for row in state.get("marks", [])
        ]
        return engine


def process(record: dict) -> dict:
    """Single-record adapter kept for the fixture pipeline."""
    rec = dict(record)
    rec.setdefault("_stage_backfill", "ok")
    return rec
