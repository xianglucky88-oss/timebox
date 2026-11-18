"""Coverage/gap planning: interval claims, four states, no max-cursor inference."""

import pytest

from src.timebox import gapfill as G
from src.timebox.timestep import interval, parse_instant


def claim(state, start, end, revision=0, reason=None, source="s"):
    return G.CoverageSlice(
        source=source,
        start=parse_instant(start),
        end=parse_instant(end),
        state=state,
        revision=revision,
        reason=reason,
    )


def test_all_four_states_are_recorded_distinctly():
    slices = [
        claim(G.CONFIRMED_DATA, "2026-11-01T00:00:00Z", "2026-11-01T01:00:00Z"),
        claim(G.CONFIRMED_EMPTY, "2026-11-01T01:00:00Z", "2026-11-01T02:00:00Z"),
        claim(G.UNRECOVERABLE, "2026-11-01T02:00:00Z", "2026-11-01T03:00:00Z"),
    ]
    report = G.resolve_coverage(
        interval("2026-11-01T00:00:00Z", "2026-11-01T05:00:00Z"), slices
    )
    assert report.data == [interval("2026-11-01T00:00:00Z", "2026-11-01T01:00:00Z")]
    assert report.empty == [interval("2026-11-01T01:00:00Z", "2026-11-01T02:00:00Z")]
    assert report.unrecoverable == [interval("2026-11-01T02:00:00Z", "2026-11-01T03:00:00Z")]
    assert report.pending == [interval("2026-11-01T03:00:00Z", "2026-11-01T05:00:00Z")]
    assert report.state_at(parse_instant("2026-11-01T01:30:00Z")) == G.CONFIRMED_EMPTY


def test_empty_is_not_pending_but_also_not_data():
    plan = G.plan(
        "2026-11-01T00:00:00Z",
        "2026-11-01T02:00:00Z",
        [claim(G.CONFIRMED_EMPTY, "2026-11-01T00:00:00Z", "2026-11-01T01:00:00Z")],
    )
    assert plan.work == [interval("2026-11-01T01:00:00Z", "2026-11-01T02:00:00Z")]
    assert plan.report.data == []


def test_later_confirmed_slice_cannot_mask_earlier_gap():
    slices = [
        claim(G.PENDING, "2026-11-01T00:00:00Z", "2026-11-01T02:00:00Z", reason="retryable_error"),
        claim(G.CONFIRMED_DATA, "2026-11-01T02:00:00Z", "2026-11-01T10:00:00Z"),
    ]
    plan = G.plan("2026-11-01T00:00:00Z", "2026-11-01T10:00:00Z", slices)
    assert plan.work == [interval("2026-11-01T00:00:00Z", "2026-11-01T02:00:00Z")]
    assert plan.report.complete is False


def test_later_success_does_not_raise_coverage_of_earlier_gap():
    before = G.plan(
        "2026-11-01T00:00:00Z",
        "2026-11-01T04:00:00Z",
        [claim(G.CONFIRMED_DATA, "2026-11-01T02:00:00Z", "2026-11-01T04:00:00Z")],
    )
    after = G.plan(
        "2026-11-01T00:00:00Z",
        "2026-11-01T04:00:00Z",
        [
            claim(G.CONFIRMED_DATA, "2026-11-01T02:00:00Z", "2026-11-01T04:00:00Z"),
            claim(G.CONFIRMED_DATA, "2026-11-01T03:00:00Z", "2026-11-01T04:00:00Z"),
        ],
    )
    assert before.work == after.work


def test_revision_mark_invalidates_only_the_affected_range():
    slices = [claim(G.CONFIRMED_DATA, "2026-11-01T00:00:00Z", "2026-11-01T06:00:00Z", revision=1)]
    marks = [G.RevisionMark("s", parse_instant("2026-11-01T02:00:00Z"), parse_instant("2026-11-01T04:00:00Z"), 2)]
    plan = G.plan(
        "2026-11-01T00:00:00Z", "2026-11-01T06:00:00Z", slices, marks=marks
    )
    assert plan.work == [interval("2026-11-01T02:00:00Z", "2026-11-01T04:00:00Z")]
    assert plan.report.state_at(parse_instant("2026-11-01T05:00:00Z")) == G.CONFIRMED_DATA
    stale_seg = next(
        seg for seg in plan.report.segments if seg.start == parse_instant("2026-11-01T02:00:00Z")
    )
    assert stale_seg.reason == "stale_revision"


def test_revision_mark_that_is_already_satisfied_changes_nothing():
    slices = [claim(G.CONFIRMED_DATA, "2026-11-01T00:00:00Z", "2026-11-01T02:00:00Z", revision=5)]
    marks = [G.RevisionMark("s", parse_instant("2026-11-01T00:00:00Z"), parse_instant("2026-11-01T02:00:00Z"), 5)]
    plan = G.plan("2026-11-01T00:00:00Z", "2026-11-01T02:00:00Z", slices, marks=marks)
    assert plan.work == []


def test_data_beats_empty_at_same_point_and_revision():
    slices = [
        claim(G.CONFIRMED_EMPTY, "2026-11-01T00:00:00Z", "2026-11-01T02:00:00Z"),
        claim(G.CONFIRMED_DATA, "2026-11-01T00:00:00Z", "2026-11-01T02:00:00Z"),
    ]
    plan = G.plan("2026-11-01T00:00:00Z", "2026-11-01T02:00:00Z", slices)
    assert plan.report.data == [interval("2026-11-01T00:00:00Z", "2026-11-01T02:00:00Z")]
    assert plan.report.empty == []


def test_unrecoverable_can_be_retried_on_request():
    slices = [claim(G.UNRECOVERABLE, "2026-11-01T00:00:00Z", "2026-11-01T02:00:00Z", reason="gone")]
    default = G.plan("2026-11-01T00:00:00Z", "2026-11-01T02:00:00Z", slices)
    retry = G.plan(
        "2026-11-01T00:00:00Z",
        "2026-11-01T02:00:00Z",
        slices,
        retry_unrecoverable=True,
    )
    assert default.work == []
    assert retry.work == [interval("2026-11-01T00:00:00Z", "2026-11-01T02:00:00Z")]


def test_report_serializes_all_states_for_query():
    plan = G.plan(
        "2026-11-01T00:00:00Z",
        "2026-11-01T04:00:00Z",
        [
            claim(G.CONFIRMED_DATA, "2026-11-01T00:00:00Z", "2026-11-01T01:00:00Z"),
            claim(G.CONFIRMED_EMPTY, "2026-11-01T01:00:00Z", "2026-11-01T02:00:00Z"),
            claim(G.UNRECOVERABLE, "2026-11-01T02:00:00Z", "2026-11-01T03:00:00Z"),
        ],
    )
    dump = plan.report.as_dict()
    assert dump["complete"] is False
    assert [seg["state"] for seg in dump["segments"]] == [
        G.CONFIRMED_DATA,
        G.CONFIRMED_EMPTY,
        G.UNRECOVERABLE,
        G.PENDING,
    ]
