"""Merge history and realtime observations by business version and evidence.

Resolution rules (all deterministic and recorded):

1. **Business version wins.**  A higher ``version`` always supersedes a lower
   one, whether it comes from backfill or realtime.  A lower version is a
   stale resend: it is acknowledged as ``stale`` and kept in history, never
   applied over a newer version.
2. **Same version + same content** is a duplicate redelivery.  It is a no-op,
   which keeps retries and overlapping realtime windows idempotent.
3. **Same version + different content** is a conflict.  Nothing is silently
   dropped: the authoritative-history value (default: the backfill origin) is
   used provisionally *and* the entity is queued for review.  This is the
   trade-off: history is authoritative for what already happened, realtime is
   preferred for freshness once it carries a newer version, and anything that
   contradicts at equal version is neither trusted blindly nor discarded.
4. **Deletes are versions too.**  A tombstone only wins if its version is at
   least the live record's version.  A strictly newer live record resurrects.
5. **Evidence is retained.**  Every observation keeps source, origin,
   revision, observation time, and a content digest.  Old inputs are never
   overwritten in the history, so the decision can always be replayed.

``process(record)`` remains the single-record adapter for the fixture pipeline.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable

from .timestep import format_instant, parse_instant, to_utc

ORIGINS = ("backfill", "realtime")

APPLIED = "applied"
DUPLICATE = "duplicate"
STALE = "stale"
CONFLICT = "conflict"
RESURRECTED = "resurrected"
DELETED = "deleted"
TOMBSTONE_HELD = "tombstone_held"

# Fields that describe the observation itself rather than the entity.
_META_FIELDS = frozenset(
    {
        "version",
        "deleted",
        "source",
        "origin",
        "revision",
        "observed_at",
        "event_ts",
        "ts",
        "ts_utc",
        "evidence_id",
        "source_tz",
        "tz",
    }
)


def content_digest(payload: dict[str, Any]) -> str:
    """Stable digest of the business content, ignoring transport metadata."""
    business = {k: v for k, v in payload.items() if k not in _META_FIELDS and not k.startswith("_")}
    blob = json.dumps(business, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]


@dataclass(frozen=True)
class Observation:
    """One delivery of one business entity from one source."""

    entity: str
    version: int
    payload: dict[str, Any]
    source: str
    origin: str = "backfill"
    revision: int = 0
    observed_at: datetime | None = None
    event_ts: datetime | None = None
    deleted: bool = False

    def __post_init__(self) -> None:
        if self.origin not in ORIGINS:
            raise ValueError(f"unknown origin: {self.origin!r}")
        if self.version < 0:
            raise ValueError("version must be non-negative")
        if self.observed_at is not None:
            object.__setattr__(self, "observed_at", to_utc(self.observed_at))
        if self.event_ts is not None:
            object.__setattr__(self, "event_ts", to_utc(self.event_ts))

    @property
    def digest(self) -> str:
        # A tombstone's content is its deletion marker.
        if self.deleted:
            return "deleted"
        return content_digest(self.payload)

    @property
    def evidence_id(self) -> str:
        return f"{self.source}:{self.origin}:v{self.version}:{self.digest[:12]}"

    def evidence(self) -> dict[str, Any]:
        return {
            "evidence_id": self.evidence_id,
            "source": self.source,
            "origin": self.origin,
            "version": self.version,
            "revision": self.revision,
            "digest": self.digest,
            "deleted": self.deleted,
            "observed_at": format_instant(self.observed_at) if self.observed_at else None,
            "event_ts": format_instant(self.event_ts) if self.event_ts else None,
        }

    def as_dict(self) -> dict[str, Any]:
        return {
            "entity": self.entity,
            "version": self.version,
            "payload": dict(self.payload),
            "source": self.source,
            "origin": self.origin,
            "revision": self.revision,
            "observed_at": format_instant(self.observed_at) if self.observed_at else None,
            "event_ts": format_instant(self.event_ts) if self.event_ts else None,
            "deleted": self.deleted,
            "evidence": self.evidence(),
        }

    @classmethod
    def from_dict(cls, row: dict[str, Any]) -> "Observation":
        return cls(
            entity=str(row["entity"]),
            version=int(row["version"]),
            payload=dict(row.get("payload") or {}),
            source=str(row["source"]),
            origin=str(row.get("origin") or "backfill"),
            revision=int(row.get("revision") or 0),
            observed_at=_parse(row.get("observed_at")),
            event_ts=_parse(row.get("event_ts")),
            deleted=bool(row.get("deleted")),
        )


def _parse(value: str | None) -> datetime | None:
    if not value:
        return None
    return parse_instant(value)


@dataclass
class EntityState:
    """Current winner plus the full retained history for one entity."""

    entity: str
    version: int = -1
    deleted: bool = True
    payload: dict[str, Any] = field(default_factory=dict)
    source: str | None = None
    origin: str | None = None
    revision: int = 0
    evidence: dict[str, Any] | None = None
    history: list[dict[str, Any]] = field(default_factory=list)
    conflicts: list[dict[str, Any]] = field(default_factory=list)

    @property
    def exists(self) -> bool:
        return not self.deleted and self.version >= 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "entity": self.entity,
            "version": self.version,
            "deleted": self.deleted,
            "exists": self.exists,
            "payload": dict(self.payload),
            "source": self.source,
            "origin": self.origin,
            "revision": self.revision,
            "evidence": dict(self.evidence) if self.evidence else None,
            "conflicts": list(self.conflicts),
            "history": list(self.history),
        }

    @classmethod
    def from_dict(cls, row: dict[str, Any]) -> "EntityState":
        return cls(
            entity=str(row["entity"]),
            version=int(row.get("version", -1)),
            deleted=bool(row.get("deleted", True)),
            payload=dict(row.get("payload") or {}),
            source=row.get("source"),
            origin=row.get("origin"),
            revision=int(row.get("revision") or 0),
            evidence=row.get("evidence"),
            history=list(row.get("history") or []),
            conflicts=list(row.get("conflicts") or []),
        )


@dataclass
class MergeResult:
    entity: str
    action: str
    applied: bool
    state: EntityState
    observation: Observation
    review: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "entity": self.entity,
            "action": self.action,
            "applied": self.applied,
            "review": self.review,
            "evidence": self.observation.evidence(),
            "winner_version": self.state.version,
            "winner_source": self.state.source,
            "winner_origin": self.state.origin,
        }


class Merger:
    """Version-aware merge state with retained evidence and a review queue."""

    def __init__(self, *, authoritative_origin: str = "backfill"):
        if authoritative_origin not in ORIGINS:
            raise ValueError(f"unknown authoritative origin: {authoritative_origin!r}")
        self.authoritative_origin = authoritative_origin
        self.entities: dict[str, EntityState] = {}

    # -- apply -----------------------------------------------------------
    def apply(self, observation: Observation) -> MergeResult:
        state = self.entities.setdefault(observation.entity, EntityState(observation.entity))
        state.history.append(observation.as_dict())

        current = state.version
        if observation.version < current:
            return MergeResult(
                observation.entity, STALE, False, state, observation
            )
        if observation.version == current and not state.deleted:
            if observation.deleted:
                # Tombstone at the same version as a live row:
                # treat as conflict-free deletion only if the delete is from
                # an origin at least as authoritative as the current winner.
                if self._rank(observation) >= self._rank_state(state):
                    self._install(state, observation, deleted=True)
                    return MergeResult(observation.entity, DELETED, True, state, observation)
                state.conflicts.append(
                    {
                        "kind": "delete_vs_live",
                        "version": observation.version,
                        "incoming": observation.evidence(),
                        "current": state.evidence,
                    }
                )
                return MergeResult(
                    observation.entity, CONFLICT, False, state, observation, review=True
                )
            if observation.digest == (state.evidence or {}).get("digest"):
                return MergeResult(observation.entity, DUPLICATE, False, state, observation)
            # Same version, different content: queue for review, keep the
            # authoritative-history winner provisionally.
            state.conflicts.append(
                {
                    "kind": "same_version_divergence",
                    "version": observation.version,
                    "incoming": observation.evidence(),
                    "current": state.evidence,
                }
            )
            if self._rank(observation) > self._rank_state(state):
                self._install(state, observation)
                return MergeResult(
                    observation.entity, CONFLICT, True, state, observation, review=True
                )
            return MergeResult(
                observation.entity, CONFLICT, False, state, observation, review=True
            )

        if observation.deleted:
            was_live = not state.deleted
            self._install(state, observation, deleted=True)
            action = DELETED if was_live else TOMBSTONE_HELD
            return MergeResult(observation.entity, action, was_live, state, observation)

        action = RESURRECTED if state.deleted and current >= 0 else APPLIED
        self._install(state, observation)
        return MergeResult(observation.entity, action, True, state, observation)

    def _install(
        self, state: EntityState, obs: Observation, *, deleted: bool = False
    ) -> None:
        state.version = obs.version
        state.deleted = deleted
        state.payload = {} if deleted else dict(obs.payload)
        state.source = obs.source
        state.origin = obs.origin
        state.revision = obs.revision
        state.evidence = obs.evidence()

    def _rank(self, obs: Observation) -> tuple[int, str]:
        return (1 if obs.origin == self.authoritative_origin else 0, obs.source)

    def _rank_state(self, state: EntityState) -> tuple[int, str]:
        return (
            1 if state.origin == self.authoritative_origin else 0,
            state.source or "",
        )

    # -- queries ---------------------------------------------------------
    def live(self) -> dict[str, dict[str, Any]]:
        return {e.entity: dict(e.payload) for e in self.entities.values() if e.exists}

    def tombstones(self) -> list[str]:
        return sorted(e.entity for e in self.entities.values() if e.deleted and e.version >= 0)

    def review_queue(self) -> list[dict[str, Any]]:
        return [
            {"entity": e.entity, **conflict}
            for e in self.entities.values()
            for conflict in e.conflicts
        ]

    def evidence_for(self, entity: str) -> list[dict[str, Any]]:
        state = self.entities.get(entity)
        return list(state.history) if state else []

    def dump(self) -> dict[str, Any]:
        return {
            "authoritative_origin": self.authoritative_origin,
            "entities": [e.as_dict() for e in self.entities.values()],
        }

    @classmethod
    def load(cls, state: dict[str, Any] | None) -> "Merger | None":
        if not state:
            return None
        merger = cls(authoritative_origin=state.get("authoritative_origin", "backfill"))
        for row in state.get("entities", []):
            entity = EntityState.from_dict(row)
            merger.entities[entity.entity] = entity
        return merger


def merge_observations(
    observations: Iterable[Observation], *, authoritative_origin: str = "backfill"
) -> tuple[Merger, list[MergeResult]]:
    merger = Merger(authoritative_origin=authoritative_origin)
    results = [merger.apply(obs) for obs in observations]
    return merger, results


def process(record: dict) -> dict:
    """Single-record adapter kept for the fixture pipeline."""
    rec = dict(record)
    rec.setdefault("_stage_merge", "ok")
    return rec
