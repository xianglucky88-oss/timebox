"""Cursor: per-slice confirmation, no watermark inference, verify invariants."""

from src.timebox import cursor as C, gapfill as G
from src.timebox.timestep import interval, parse_instant


def claim(state, start, end, revision=0):
    return G.CoverageSlice(
        source="s", start=parse_instant(start), end=parse_instant(end), state=state, revision=revision
    )


def test_resume_position_is_earliest_gap_not_latest_confirmed():
    c = C.CoverageCursor("s")
    c.record_claim(claim(G.CONFIRMED_DATA, "2026-11-01T00:00:00Z", "2026-11-01T01:00:00Z"))
    c.record_claim(claim(G.PENDING, "2026-11-01T01:00:00Z", "2026-11-01T02:00:00Z"))
    # Later slices confirmed far ahead: high water is 04:00, but the resume
    # position must stay at the first hole.
    c.record_claim(claim(G.CONFIRMED_DATA, "2026-11-01T03:00:00Z", "2026-11-01T04:00:00Z"))
    target = interval("2026-11-01T00:00:00Z", "2026-11-01T04:00:00Z")
    snapshot = c.snapshot(target)
    assert snapshot.high_water == parse_instant("2026-11-01T04:00:00Z")
    assert snapshot.resume_position == parse_instant("2026-11-01T01:00:00Z")
    # 01:00-02:00 is an explicit pending slice and 02:00-03:00 was never
    # claimed at all; both are reported as outstanding work.
    assert snapshot.pending == [interval("2026-11-01T01:00:00Z", "2026-11-01T03:00:00Z")]


def test_unfilled_reports_every_gap_including_earlier_ones():
    c = C.CoverageCursor("s")
    c.record_claim(claim(G.CONFIRMED_DATA, "2026-11-01T00:00:00Z", "2026-11-01T01:00:00Z"))
    c.record_claim(claim(G.CONFIRMED_DATA, "2026-11-01T02:00:00Z", "2026-11-01T03:00:00Z"))
    c.record_claim(claim(G.CONFIRMED_DATA, "2026-11-01T05:00:00Z", "2026-11-01T06:00:00Z"))
    target = interval("2026-11-01T00:00:00Z", "2026-11-01T06:00:00Z")
    assert c.unfilled(target) == [
        interval("2026-11-01T01:00:00Z", "2026-11-01T02:00:00Z"),
        interval("2026-11-01T03:00:00Z", "2026-11-01T05:00:00Z"),
    ]


def test_empty_and_unrecoverable_are_distinguishable_states():
    c = C.CoverageCursor("s")
    c.record_claim(claim(G.CONFIRMED_EMPTY, "2026-11-01T00:00:00Z", "2026-11-01T01:00:00Z"))
    c.record_claim(claim(G.UNRECOVERABLE, "2026-11-01T01:00:00Z", "2026-11-01T02:00:00Z"))
    target = interval("2026-11-01T00:00:00Z", "2026-11-01T02:00:00Z")
    assert c.unfilled(target) == []
    assert c.unrecoverable_intervals() == [interval("2026-11-01T01:00:00Z", "2026-11-01T02:00:00Z")]
    snapshot = c.snapshot(target)
    assert snapshot.complete is True
    assert snapshot.unrecoverable == [interval("2026-11-01T01:00:00Z", "2026-11-01T02:00:00Z")]


def test_cursor_round_trips_and_rebuilds_from_ledger():
    c = C.CoverageCursor("s")
    c.record_claim(claim(G.CONFIRMED_DATA, "2026-11-01T00:00:00Z", "2026-11-01T01:00:00Z", revision=2))
    c.record_claim(claim(G.PENDING, "2026-11-01T01:00:00Z", "2026-11-01T02:00:00Z", revision=2))
    restored = C.CoverageCursor.load(c.dump())
    assert restored.entries() == c.entries()
    rebuilt = C.rebuild("s", c.to_slices())
    assert rebuilt.entries() == c.entries()


def test_verify_rejects_confirmed_overlap_with_pending():
    c = C.CoverageCursor("s")
    c.record_claim(claim(G.CONFIRMED_DATA, "2026-11-01T00:00:00Z", "2026-11-01T02:00:00Z"))
    c.record_claim(claim(G.PENDING, "2026-11-01T01:00:00Z", "2026-11-01T03:00:00Z"))
    try:
        c.verify()
    except C.CursorError:
        pass
    else:  # pragma: no cover
        raise AssertionError("expected CursorError")


def test_verify_accepts_a_consistent_cursor():
    c = C.CoverageCursor("s")
    c.record_claim(claim(G.CONFIRMED_DATA, "2026-11-01T00:00:00Z", "2026-11-01T01:00:00Z"))
    c.record_claim(claim(G.CONFIRMED_EMPTY, "2026-11-01T01:00:00Z", "2026-11-01T02:00:00Z"))
    c.verify(target=interval("2026-11-01T00:00:00Z", "2026-11-01T02:00:00Z"))
