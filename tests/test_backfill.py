"""End-to-end backfill: restart, resume, revisions, dedupe, budget, retention."""

import json

import pytest

from src.timebox import backfill as BF, budget as B, gapfill as G, merge as M, timestep as T
from src.timebox.timestep import format_instant, interval, parse_instant


def make_source(table, *, name="s", revision=0, unrecoverable=()):
    """``table`` maps a day to either a list of records or a SourceResponse."""

    def responder(iv, *, revision, attempt):
        day = iv.start.date().isoformat()
        value = table.get(day, [])
        if isinstance(value, BF.SourceResponse):
            return value
        return BF.SourceResponse(
            BF.FetchOutcome.DATA if value else BF.FetchOutcome.EMPTY, observations=[
                M.Observation(
                    entity=rec["id"],
                    version=rec.get("version", 1),
                    payload=dict(rec),
                    source=name,
                    origin="backfill",
                    revision=revision,
                    event_ts=T.parse_instant(rec["ts"]),
                )
                for rec in value
            ],
            revision=revision,
        )

    return BF.StaticSource(name, responder, revision=revision, unrecoverable=unrecoverable)


def engine(source, **kwargs):
    kwargs.setdefault("unit", "day")
    kwargs.setdefault("budget", B.BudgetLedger(1000))
    return BF.BackfillEngine(source, **kwargs)


def rec(rid, ts, value=1, version=1):
    return {"id": rid, "ts": ts, "value": value, "version": version}


def test_full_range_is_confirmed_with_data_empty_and_complete_coverage():
    source = make_source(
        {
            "2026-11-01": [rec("a", "2026-11-01T01:00:00Z", 10)],
            "2026-11-02": [],
            "2026-11-03": [rec("b", "2026-11-03T05:00:00Z", 30)],
        }
    )
    e = engine(source)
    run = e.run("2026-11-01", "2026-11-04")
    assert run.done
    states = {s.interval.start.date().isoformat(): s.state for s in run.slices}
    assert states == {
        "2026-11-01": G.CONFIRMED_DATA,
        "2026-11-02": G.CONFIRMED_EMPTY,
        "2026-11-03": G.CONFIRMED_DATA,
    }
    status = e.status("2026-11-01", "2026-11-04")
    assert status["coverage"]["complete"] is True
    assert status["coverage"]["pending"] == []
    assert status["resume_position"] is None
    assert {k: v["payload"]["value"] for k, v in status["results"].items()} == {"a": 10, "b": 30}


def test_retryable_failure_leaves_gap_that_later_success_cannot_mask():
    calls = []

    def responder(iv, *, revision, attempt):
        calls.append((iv.start.date().isoformat(), attempt))
        if iv.start.day == 2:
            return BF.SourceResponse(BF.FetchOutcome.RETRYABLE, detail="503")
        return BF.SourceResponse(BF.FetchOutcome.EMPTY)

    e = engine(BF.StaticSource("s", responder))
    e.run("2026-11-01", "2026-11-04")
    status = e.status("2026-11-01", "2026-11-04")
    assert [p["start"][:10] for p in status["coverage"]["pending"]] == ["2026-11-02"]
    assert status["resume_position"] == "2026-11-02T00:00:00.000000Z"
    assert status["coverage"]["complete"] is False
    # Day 3 succeeded, but that must not raise coverage of day 2.
    assert status["coverage"]["covered"] == [
        {"start": "2026-11-01T00:00:00.000000Z", "end": "2026-11-02T00:00:00.000000Z"},
        {"start": "2026-11-03T00:00:00.000000Z", "end": "2026-11-04T00:00:00.000000Z"},
    ]


def test_restart_resumes_only_unconfirmed_ranges():
    attempts = {}

    def responder(iv, *, revision, attempt):
        day = iv.start.date().isoformat()
        attempts[day] = attempts.get(day, 0) + 1
        if day == "2026-11-02" and attempts[day] == 1:
            return BF.SourceResponse(BF.FetchOutcome.RETRYABLE)
        return BF.SourceResponse(BF.FetchOutcome.EMPTY)

    source = BF.StaticSource("s", responder)
    e = engine(source)
    e.run("2026-11-01", "2026-11-04")

    restored = BF.BackfillEngine.load(json.loads(json.dumps(e.dump())), source)
    run = restored.run("2026-11-01", "2026-11-04")
    fetched = [s.interval.start.date().isoformat() for s in run.slices]
    assert fetched == ["2026-11-02"]
    status = restored.status("2026-11-01", "2026-11-04")
    assert status["coverage"]["complete"] is True
    restored.verify("2026-11-01", "2026-11-04")


def test_revision_mark_refetches_only_affected_range_and_updates_versions():
    source = make_source({"2026-11-01": [], "2026-11-02": [], "2026-11-03": []})
    e = engine(source)
    e.run("2026-11-01", "2026-11-04")

    e.mark_revision("2026-11-01T06:00:00Z", "2026-11-01T12:00:00Z", 7)
    plan = e.plan("2026-11-01", "2026-11-04")
    assert plan.work == [interval("2026-11-01T06:00:00Z", "2026-11-01T12:00:00Z")]

    run = e.run("2026-11-01", "2026-11-04")
    fetched = [(format_instant(s.interval.start), format_instant(s.interval.end)) for s in run.slices]
    assert fetched == [("2026-11-01T06:00:00.000000Z", "2026-11-01T12:00:00.000000Z")]
    assert all(s.revision == 7 for s in run.slices)


def test_partial_revision_refetch_layers_over_wider_confirmed_slice():
    def responder(iv, *, revision, attempt):
        obs = (
            [
                M.Observation(
                    "e",
                    2,
                    {"id": "e", "v": 2},
                    source="s",
                    origin="backfill",
                    revision=revision,
                    event_ts=parse_instant("2026-11-01T08:00:00Z"),
                )
            ]
            if iv.start.hour == 6
            else []
        )
        return BF.SourceResponse(
            BF.FetchOutcome.DATA if obs else BF.FetchOutcome.EMPTY,
            observations=obs,
            revision=revision,
        )

    e = engine(BF.StaticSource("s", responder))
    e.run("2026-11-01", "2026-11-02")
    e.mark_revision("2026-11-01T06:00:00Z", "2026-11-01T12:00:00Z", 2)
    run = e.run("2026-11-01", "2026-11-02")
    assert len(run.slices) == 1
    assert run.slices[0].interval == interval("2026-11-01T06:00:00Z", "2026-11-01T12:00:00Z")
    e.verify("2026-11-01", "2026-11-02")
    report = e.plan("2026-11-01", "2026-11-02").report
    assert report.complete is True
    assert report.state_at(parse_instant("2026-11-01T03:00:00Z")) == G.CONFIRMED_EMPTY
    assert report.state_at(parse_instant("2026-11-01T08:00:00Z")) == G.CONFIRMED_DATA


def test_outstanding_claim_keeps_its_extent_on_retry():
    calls = []

    def responder(iv, *, revision, attempt):
        hours = (iv.end - iv.start).total_seconds() / 3600
        calls.append(hours)
        if hours >= 24:
            return BF.SourceResponse(BF.FetchOutcome.RETRYABLE)
        return BF.SourceResponse(BF.FetchOutcome.EMPTY)

    e = engine(BF.StaticSource("s", responder), unit="day")
    e.run("2026-11-01", "2026-11-02")              # day-sized attempt fails
    run = e.run("2026-11-01", "2026-11-02", unit="hour")
    # The outstanding day claim is retried as a day claim, not refined mid-flight.
    assert [(s.interval.end - s.interval.start).total_seconds() / 3600 for s in run.slices] == [24]
    assert [s.attempts for s in run.slices] == [2]
    e.verify("2026-11-01", "2026-11-02")


def test_budget_dedupe_across_retries_unknown_and_replay():
    state = {"n": 0}

    def responder(iv, *, revision, attempt):
        state["n"] += 1
        if state["n"] < 3:
            return BF.SourceResponse(BF.FetchOutcome.UNKNOWN, detail="timeout")
        return BF.SourceResponse(BF.FetchOutcome.EMPTY)

    e = engine(BF.StaticSource("s", responder))
    e.run("2026-11-01", "2026-11-02")
    e.run("2026-11-01", "2026-11-02")
    assert e.budget.snapshot()["held"] == 1  # one outstanding hold for the unknown outcome
    e.run("2026-11-01", "2026-11-02")
    assert e.budget.snapshot()["used"] == 1  # the eventual success consumes exactly one unit
    assert e.budget.snapshot()["held"] == 0


def test_budget_exhaustion_stops_without_losing_progress():
    def responder(iv, *, revision, attempt):
        return BF.SourceResponse(BF.FetchOutcome.EMPTY)

    e = engine(BF.StaticSource("s", responder), budget=B.BudgetLedger(2))
    run = e.run("2026-11-01", "2026-11-10")
    assert run.budget_exhausted is True
    assert [s.action for s in run.slices] == ["settled", "settled", "deferred"]
    assert run.slices[-1].state == G.PENDING
    assert run.slices[-1].op_id is None  # nothing was reserved for the deferred slice
    status = e.status("2026-11-01", "2026-11-10")
    assert status["coverage"]["complete"] is False
    assert status["resume_position"] == "2026-11-03T00:00:00.000000Z"
    # A later run with fresh budget continues from the resume position.
    e.budget = B.BudgetLedger(100)
    resumed = e.run("2026-11-01", "2026-11-10")
    assert [s.interval.start.date().isoformat() for s in resumed.slices][0] == "2026-11-03"


def test_unrecoverable_history_is_kept_distinct_and_not_retried_by_default():
    source = make_source(
        {"2026-11-01": [], "2026-11-02": [], "2026-11-03": []},
        unrecoverable=[interval("2026-11-02T00:00:00Z", "2026-11-03T00:00:00Z")],
    )
    e = engine(source)
    run = e.run("2026-11-01", "2026-11-04")
    assert [s.state for s in run.slices] == [
        G.CONFIRMED_EMPTY,
        G.UNRECOVERABLE,
        G.CONFIRMED_EMPTY,
    ]
    status = e.status("2026-11-01", "2026-11-04")
    assert status["coverage"]["unrecoverable"] == [
        {"start": "2026-11-02T00:00:00.000000Z", "end": "2026-11-03T00:00:00.000000Z"}
    ]
    assert status["coverage"]["complete"] is True
    assert status["coverage"]["pending"] == []
    # Explicit retry is still possible.
    assert e.plan("2026-11-01", "2026-11-04", retry_unrecoverable=True).work == [
        interval("2026-11-02T00:00:00Z", "2026-11-03T00:00:00Z")
    ]


def test_hour_slices_and_day_slices_produce_identical_coverage():
    def responder(iv, *, revision, attempt):
        return BF.SourceResponse(BF.FetchOutcome.EMPTY)

    hourly = engine(BF.StaticSource("s", responder), unit="hour")
    hourly.run("2026-11-01", "2026-11-03", unit="hour")
    daily = engine(BF.StaticSource("s", responder), unit="day")
    daily.run("2026-11-01", "2026-11-03", unit="day")

    h = hourly.cursor.confirmed_intervals()
    d = daily.cursor.confirmed_intervals()
    assert T.same_coverage(h, d)


def test_realtime_and_backfill_share_budget_without_double_charging():
    def responder(iv, *, revision, attempt):
        return BF.SourceResponse(BF.FetchOutcome.EMPTY)

    e = engine(BF.StaticSource("s", responder), budget=B.BudgetLedger(3, realtime_reserve=1))
    backfill_run = e.run("2026-11-01", "2026-11-10")
    assert backfill_run.budget_exhausted is True
    assert e.budget.snapshot()["used"] == 2  # capacity 3 - reserve 1
    # Realtime keeps its reserved lane.
    assert e.budget.available("realtime") == 1
    e.ingest_realtime(
        [M.Observation("x", 1, {"id": "x", "v": 1}, source="live", origin="realtime")],
        op_id="rt-1",
    )
    assert e.budget.available("realtime") == 0
    ops = {t["op_id"] for t in e.budget.transactions()}
    assert len(ops) == len([t for t in e.budget.transactions() if t["kind"] == "reserve"])


def test_merge_conflict_is_reported_in_status_review_queue():
    table = {"2026-11-01": [rec("a", "2026-11-01T01:00:00Z", 1)]}
    e = engine(make_source(table))
    e.run("2026-11-01", "2026-11-02")
    e.ingest_realtime(
        [
            M.Observation(
                "a",
                1,
                {"id": "a", "ts": "2026-11-01T01:00:00Z", "value": 999, "version": 1},
                source="live",
                origin="realtime",
            )
        ],
        op_id="rt-conflict",
    )
    status = e.status("2026-11-01", "2026-11-02")
    assert len(status["review_queue"]) == 1
    assert status["results"]["a"]["payload"]["value"] == 1  # history stays authoritative


def test_dump_retains_old_inputs_and_restart_is_lossless():
    table = {"2026-11-01": [rec("a", "2026-11-01T01:00:00Z", 1)]}
    e = engine(make_source(table))
    e.run("2026-11-01", "2026-11-02")
    # A revision brings a new value for the same entity.
    e.ingest_realtime(
        [M.Observation("a", 2, {"id": "a", "value": 2}, source="live", origin="realtime")],
        op_id="rt",
    )
    e.mark_revision("2026-11-01", "2026-11-02", 3)
    dump = json.loads(json.dumps(e.dump()))
    assert [h["version"] for h in dump["merge"]["entities"][0]["history"]] == [1, 2]

    restored = BF.BackfillEngine.load(dump, make_source(table))
    assert restored.status("2026-11-01", "2026-11-02")["results"]["a"]["payload"]["value"] == 2
    assert restored.plan("2026-11-01", "2026-11-02").work == [interval("2026-11-01", "2026-11-02")]
