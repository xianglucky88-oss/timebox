"""Merge: business versions, evidence retention, resend/revision/delete."""

from datetime import datetime, timezone

from src.timebox import merge as M


def obs(entity="a", version=1, value=1, origin="backfill", source="hist", deleted=False, revision=0, at=None):
    return M.Observation(
        entity=entity,
        version=version,
        payload={} if deleted else {"id": entity, "value": value},
        source=source,
        origin=origin,
        revision=revision,
        deleted=deleted,
        observed_at=at,
    )


def test_newer_version_wins_over_older_regardless_of_origin():
    m = M.Merger()
    m.apply(obs(version=2, value=2, origin="realtime"))
    result = m.apply(obs(version=1, value=1, origin="backfill"))
    assert result.action == M.STALE
    assert m.live()["a"]["value"] == 2


def test_realtime_newer_version_supersedes_authoritative_history():
    m = M.Merger()
    m.apply(obs(version=1, value=1, origin="backfill"))
    m.apply(obs(version=2, value=9, origin="realtime"))
    assert m.live()["a"]["value"] == 9
    assert m.entities["a"].origin == "realtime"


def test_same_version_same_content_is_an_idempotent_redelivery():
    m = M.Merger()
    first = m.apply(obs(version=3, value=7))
    second = m.apply(obs(version=3, value=7))
    assert first.action == M.APPLIED
    assert second.action == M.DUPLICATE
    assert second.applied is False
    assert m.review_queue() == []


def test_same_version_different_content_is_queued_for_review_not_dropped():
    m = M.Merger(authoritative_origin="backfill")
    m.apply(obs(version=3, value=7, origin="backfill"))
    result = m.apply(obs(version=3, value=8, origin="realtime"))
    assert result.action == M.CONFLICT
    assert result.review is True
    # History stays authoritative provisionally, and both sides are retained.
    assert m.live()["a"]["value"] == 7
    queue = m.review_queue()
    assert len(queue) == 1
    assert queue[0]["kind"] == "same_version_divergence"
    assert queue[0]["incoming"]["digest"] != queue[0]["current"]["digest"]


def test_stale_delete_does_not_remove_live_record():
    m = M.Merger()
    m.apply(obs(version=5, value=5))
    result = m.apply(obs(version=4, deleted=True))
    assert result.action == M.STALE
    assert m.live()["a"]["value"] == 5


def test_current_version_delete_removes_and_is_kept_as_tombstone():
    m = M.Merger()
    m.apply(obs(version=5, value=5))
    result = m.apply(obs(version=5, deleted=True, source="hist"))
    assert result.action in (M.DELETED, M.CONFLICT)
    assert "a" not in m.live()


def test_newer_live_record_resurrects_a_tombstone():
    m = M.Merger()
    m.apply(obs(version=5, value=5))
    m.apply(obs(version=6, deleted=True, origin="realtime"))
    assert "a" not in m.live()
    result = m.apply(obs(version=7, value=7))
    assert result.action == M.RESURRECTED
    assert m.live()["a"]["value"] == 7


def test_all_inputs_are_retained_as_evidence():
    m = M.Merger()
    for version in (1, 2, 3):
        m.apply(obs(version=version, value=version))
    history = m.evidence_for("a")
    assert [h["version"] for h in history] == [1, 2, 3]
    assert all("digest" in h["evidence"] for h in history)
    assert all(h["evidence"]["origin"] == "backfill" for h in history)


def test_merge_state_round_trips_through_dump_load():
    m = M.Merger()
    m.apply(obs(version=1, value=1))
    m.apply(obs(version=1, value=2, origin="realtime"))
    restored = M.Merger.load(m.dump())
    assert restored.live()["a"]["value"] == m.live()["a"]["value"]
    assert len(restored.review_queue()) == len(m.review_queue())
    assert restored.evidence_for("a") == m.evidence_for("a")


def test_business_content_digest_ignores_transport_metadata():
    a = M.Observation("a", 1, {"id": "a", "value": 1, "ts": "2026-11-01T00:00:00Z"}, source="s1")
    b = M.Observation("a", 1, {"id": "a", "value": 1, "ts": "2026-11-02T00:00:00Z"}, source="s2")
    assert a.digest == b.digest
    assert a.evidence_id != b.evidence_id  # source is part of the evidence id
