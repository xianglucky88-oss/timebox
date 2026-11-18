"""Shared request budget for backfill and the realtime stream.

The budget is a per-period (per local day) pool.  Every access is an explicit
operation with a caller-owned ``op_id``, and every state change is an
append-only ledger transaction, so any charge can be explained after the fact:

``reserve``
    Hold ``amount`` before the request is sent.  Reserving the same ``op_id``
    again is a no-op: a retried request never holds twice.
``settle``
    Replace the hold with the actual cost.  Settling the same ``op_id`` with
    the same cost is a no-op; with a different cost it is a conflict.
``refund``
    Release a hold that definitely did not consume anything.  Refunding twice
    is a no-op.  Settled operations cannot be refunded.
``mark_unknown``
    The request may or may not have been served (timeout / connection dropped
    mid-response).  The hold is kept, so the budget stays conservative, and a
    retry of the same ``op_id`` reuses that single hold.  Unknown holds expire
    after ``hold_ttl`` and are refunded with an explicit reason, so they can
    never block the realtime lane indefinitely.

Lane isolation: the realtime lane may draw on the whole capacity; the backfill
lane may only draw on ``capacity - realtime_reserve``.  Backfill can therefore
never starve realtime, regardless of how much history it wants to fetch.

Day boundaries are evaluated in the budget timezone, and a reservation is
charged to the period it was created in even when it settles after midnight
(``cross-day`` settlements stay attached to their original day).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

UTC = timezone.utc

LANES = ("backfill", "realtime")
HELD_STATES = ("held", "unknown")
TERMINAL_STATES = ("settled", "refunded")


class BudgetError(Exception):
    """Base class for budget failures."""


class BudgetExceeded(BudgetError):
    """The requested reservation does not fit in the lane's remaining budget."""

    def __init__(self, lane: str, period: str, requested: int, available: int):
        self.lane = lane
        self.period = period
        self.requested = requested
        self.available = available
        super().__init__(
            f"{lane} lane needs {requested} in {period} but only {available} available"
        )


class ConflictingSettlement(BudgetError):
    """A settled or refunded operation received an incompatible outcome."""


@dataclass
class BudgetEntry:
    op_id: str
    lane: str
    period: str
    amount: int
    state: str = "held"
    actual: int | None = None
    created_at: datetime | None = None
    note: str = ""
    txns: list[dict[str, Any]] = field(default_factory=list)

    @property
    def outstanding(self) -> int:
        """Units still held against the pool."""
        return self.amount if self.state in HELD_STATES else 0

    @property
    def consumed(self) -> int:
        return self.actual if self.state == "settled" else 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "op_id": self.op_id,
            "lane": self.lane,
            "period": self.period,
            "amount": self.amount,
            "state": self.state,
            "actual": self.actual,
            "created_at": _fmt(self.created_at),
            "note": self.note,
            "txns": list(self.txns),
        }

    @classmethod
    def from_dict(cls, row: dict[str, Any]) -> "BudgetEntry":
        return cls(
            op_id=row["op_id"],
            lane=row["lane"],
            period=row["period"],
            amount=int(row["amount"]),
            state=row["state"],
            actual=row.get("actual"),
            created_at=_parse(row.get("created_at")),
            note=row.get("note", ""),
            txns=list(row.get("txns", [])),
        )


def _fmt(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _parse(value: str | None) -> datetime | None:
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).astimezone(UTC)


def _now(at: datetime | None) -> datetime:
    value = at or datetime.now(UTC)
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


class BudgetLedger:
    """Idempotent reservation/settlement ledger with lane isolation."""

    def __init__(
        self,
        capacity: int,
        *,
        realtime_reserve: int = 0,
        tz: str = "UTC",
        hold_ttl: timedelta | None = timedelta(hours=6),
    ):
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        if realtime_reserve < 0 or realtime_reserve > capacity:
            raise ValueError("realtime_reserve must be within [0, capacity]")
        self.capacity = int(capacity)
        self.realtime_reserve = int(realtime_reserve)
        self.tz = tz
        self.hold_ttl = hold_ttl
        self._entries: dict[str, BudgetEntry] = {}

    # -- helpers ---------------------------------------------------------
    def period_of(self, at: datetime | None = None) -> str:
        value = _now(at)
        return value.astimezone(ZoneInfo(self.tz)).date().isoformat()

    def _limit(self, lane: str) -> int:
        if lane == "realtime":
            return self.capacity
        if lane == "backfill":
            return self.capacity - self.realtime_reserve
        raise ValueError(f"unknown lane: {lane!r}")

    def _lane_committed(self, lane: str, period: str) -> int:
        """Held + settled units for one lane in one period."""
        return sum(
            e.outstanding + e.consumed
            for e in self._entries.values()
            if e.period == period and e.lane == lane
        )

    def _available(self, lane: str, period: str) -> int:
        if lane == "realtime":
            # Realtime may use the whole pool, but backfill can only ever
            # consume its own share (capacity - reserve).  Any backfill
            # settlement overage is bookkept as backfill debt and cannot eat
            # into the realtime reserve.
            reserved_for_backfill = self.capacity - self.realtime_reserve
            backfill_used = min(
                self._lane_committed("backfill", period), reserved_for_backfill
            )
            return max(
                0,
                self.capacity
                - self._lane_committed("realtime", period)
                - backfill_used,
            )
        return max(0, self._limit(lane) - self._lane_committed(lane, period))

    def available(self, lane: str = "backfill", at: datetime | None = None) -> int:
        if lane not in LANES:
            raise ValueError(f"unknown lane: {lane!r}")
        self.expire(at)
        return self._available(lane, self.period_of(at))

    # -- lifecycle -------------------------------------------------------
    def reserve(
        self,
        op_id: str,
        amount: int,
        *,
        lane: str = "backfill",
        at: datetime | None = None,
        note: str = "",
    ) -> tuple[BudgetEntry, bool]:
        """Hold ``amount`` for ``op_id``.  Returns ``(entry, created)``.

        Repeating a reservation for a known ``op_id`` returns the existing
        operation unchanged, which is what makes retries idempotent.
        """
        if op_id in self._entries:
            return self._entries[op_id], False
        if amount < 0:
            raise ValueError("amount must be non-negative")
        if lane not in LANES:
            raise ValueError(f"unknown lane: {lane!r}")
        self.expire(at)
        timestamp = _now(at)
        period = self.period_of(timestamp)
        free = self._available(lane, period)
        if amount > free:
            raise BudgetExceeded(lane, period, amount, free)
        entry = BudgetEntry(
            op_id=op_id,
            lane=lane,
            period=period,
            amount=int(amount),
            state="held",
            created_at=timestamp,
            note=note,
        )
        entry.txns.append(
            {
                "kind": "reserve",
                "amount": int(amount),
                "at": _fmt(timestamp),
                "period": period,
                "lane": lane,
                "note": note,
            }
        )
        self._entries[op_id] = entry
        return entry, True

    def settle(
        self,
        op_id: str,
        actual: int | None = None,
        *,
        at: datetime | None = None,
        note: str = "",
    ) -> BudgetEntry:
        """Settle a hold at its real cost.

        ``actual`` may exceed the reserved amount (a request can turn out to
        cost more than the estimate).  The overage is recorded explicitly as a
        ledger transaction instead of being dropped.  Very large overages are
        rare by design; they still cannot starve realtime because the realtime
        lane has its own reserve and lanes are counted separately.
        """
        entry = self._entries.get(op_id)
        if entry is None:
            raise BudgetError(f"unknown operation: {op_id}")
        timestamp = _now(at)
        cost = entry.amount if actual is None else int(actual)
        if cost < 0:
            raise ValueError("settled cost must be non-negative")
        if entry.state == "settled":
            if entry.actual == cost:
                return entry  # idempotent replay
            raise ConflictingSettlement(
                f"{op_id} already settled at {entry.actual}, got {cost}"
            )
        if entry.state == "refunded":
            raise ConflictingSettlement(f"{op_id} was refunded; cannot settle")
        overage = max(0, cost - entry.amount)
        entry.state = "settled"
        entry.actual = cost
        entry.txns.append(
            {
                "kind": "settle",
                "amount": cost,
                "reserved": entry.amount,
                "released": max(0, entry.amount - cost),
                "overage": overage,
                "at": _fmt(timestamp),
                "period": entry.period,
                "lane": entry.lane,
                "note": note,
            }
        )
        return entry

    def refund(
        self,
        op_id: str,
        *,
        reason: str = "not_consumed",
        at: datetime | None = None,
    ) -> BudgetEntry:
        entry = self._entries.get(op_id)
        if entry is None:
            raise BudgetError(f"unknown operation: {op_id}")
        if entry.state == "refunded":
            return entry
        if entry.state == "settled":
            raise ConflictingSettlement(f"{op_id} already settled; cannot refund")
        timestamp = _now(at)
        entry.state = "refunded"
        entry.txns.append(
            {
                "kind": "refund",
                "amount": entry.amount,
                "at": _fmt(timestamp),
                "period": entry.period,
                "lane": entry.lane,
                "reason": reason,
            }
        )
        return entry

    def mark_unknown(
        self, op_id: str, *, at: datetime | None = None, note: str = ""
    ) -> BudgetEntry:
        """Record that the request outcome is unknown; keep the hold."""
        entry = self._entries.get(op_id)
        if entry is None:
            raise BudgetError(f"unknown operation: {op_id}")
        if entry.state == "held":
            timestamp = _now(at)
            entry.state = "unknown"
            entry.note = note or entry.note
            entry.txns.append(
                {
                    "kind": "unknown",
                    "amount": entry.amount,
                    "at": _fmt(timestamp),
                    "period": entry.period,
                    "lane": entry.lane,
                    "reason": "unknown_outcome",
                    "note": note,
                }
            )
        return entry

    def expire(self, at: datetime | None = None) -> list[str]:
        """Refund holds older than ``hold_ttl``.  Returns the affected op ids."""
        if self.hold_ttl is None:
            return []
        timestamp = _now(at)
        released: list[str] = []
        for entry in list(self._entries.values()):
            if entry.state not in HELD_STATES or entry.created_at is None:
                continue
            if timestamp - entry.created_at < self.hold_ttl:
                continue
            reason = (
                "unknown_expired" if entry.state == "unknown" else "hold_expired"
            )
            self.refund(entry.op_id, reason=reason, at=timestamp)
            released.append(entry.op_id)
        return released

    # -- observability ---------------------------------------------------
    def get(self, op_id: str) -> BudgetEntry | None:
        return self._entries.get(op_id)

    def explain(self, op_id: str) -> dict[str, Any] | None:
        entry = self._entries.get(op_id)
        if entry is None:
            return None
        return {
            "entry": entry.as_dict(),
            "outstanding": entry.outstanding,
            "consumed": entry.consumed,
            "replay_safe": entry.state in TERMINAL_STATES,
        }

    def transactions(self) -> list[dict[str, Any]]:
        txns: list[dict[str, Any]] = []
        for entry in self._entries.values():
            for txn in entry.txns:
                txns.append({"op_id": entry.op_id, **txn})
        txns.sort(key=lambda t: (t["at"], t["op_id"], t["kind"]))
        return txns

    def snapshot(self, at: datetime | None = None) -> dict[str, Any]:
        self.expire(at)
        period = self.period_of(at)
        entries = [
            e.as_dict() for e in self._entries.values() if e.period == period
        ]
        used = sum(e.consumed for e in self._entries.values() if e.period == period)
        held = sum(e.outstanding for e in self._entries.values() if e.period == period)
        return {
            "period": period,
            "tz": self.tz,
            "capacity": self.capacity,
            "realtime_reserve": self.realtime_reserve,
            "used": used,
            "held": held,
            "available_backfill": self.available("backfill", at),
            "available_realtime": self.available("realtime", at),
            "entries": entries,
        }

    # -- persistence -----------------------------------------------------
    def dump(self) -> dict[str, Any]:
        return {
            "capacity": self.capacity,
            "realtime_reserve": self.realtime_reserve,
            "tz": self.tz,
            "hold_ttl_seconds": (
                self.hold_ttl.total_seconds() if self.hold_ttl else None
            ),
            "entries": [e.as_dict() for e in self._entries.values()],
        }

    @classmethod
    def load(cls, state: dict[str, Any] | None) -> "BudgetLedger | None":
        if not state:
            return None
        ttl = state.get("hold_ttl_seconds")
        ledger = cls(
            int(state["capacity"]),
            realtime_reserve=int(state.get("realtime_reserve", 0)),
            tz=state.get("tz", "UTC"),
            hold_ttl=timedelta(seconds=ttl) if ttl is not None else None,
        )
        for row in state.get("entries", []):
            entry = BudgetEntry.from_dict(row)
            ledger._entries[entry.op_id] = entry
        return ledger


def process(record: dict) -> dict:
    """Single-record adapter kept for the fixture pipeline."""
    rec = dict(record)
    rec.setdefault("_stage_budget", "ok")
    return rec
