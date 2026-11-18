"""Time-slice conventions: half-open ranges, source timezone, DST, precision."""

from datetime import datetime, timedelta, timezone

import pytest

from src.timebox import timestep as T


def test_half_open_and_adjacent_slices_tile_exactly():
    slices = T.slice_range("2026-11-01T00:00:00Z", "2026-11-01T04:00:00Z", unit="hour")
    assert [s.start.hour for s in slices] == [0, 1, 2, 3]
    T.validate_slices(slices, *T.normalize_range("2026-11-01T00:00:00Z", "2026-11-01T04:00:00Z"))
    # A boundary instant belongs to exactly one slice (the later one).
    boundary = T.parse_instant("2026-11-01T01:00:00Z")
    assert boundary in slices[1]
    assert boundary not in slices[0]


def test_range_end_is_exclusive():
    start, end = T.normalize_range("2026-11-01T00:00:00Z", "2026-11-01T02:00:00Z")
    iv = T.Interval(start, end)
    assert iv.contains(start)
    assert iv.contains(end - timedelta(microseconds=1))
    assert not iv.contains(end)


@pytest.mark.parametrize(
    "tz,start,end",
    [
        ("UTC", "2026-11-01T00:00:00", "2026-11-05T00:00:00"),
        # Spring-forward: 2026-03-08 in America/New_York has 23 local hours.
        ("America/New_York", "2026-03-07T00:00:00", "2026-03-10T00:00:00"),
        # Fall-back: 2026-11-01 in America/New_York has 25 local hours.
        ("America/New_York", "2026-10-31T00:00:00", "2026-11-03T00:00:00"),
        ("Europe/Berlin", "2026-03-28T00:00:00", "2026-03-31T00:00:00"),
    ],
)
def test_hour_and_day_splits_cover_identically_across_dst(tz, start, end):
    hour_slices = T.slice_range(start, end, unit="hour", tz=tz)
    day_slices = T.slice_range(start, end, unit="day", tz=tz)
    s, e = T.normalize_range(start, end, tz=tz)
    T.validate_slices(hour_slices, s, e)
    T.validate_slices(day_slices, s, e)
    assert T.same_coverage(hour_slices, day_slices)
    assert T.total_seconds(hour_slices) == T.total_seconds(day_slices) == (e - s).total_seconds()


def test_dst_day_length_is_calendar_count_not_24h():
    spring = T.slice_range(
        "2026-03-08T00:00:00", "2026-03-09T00:00:00", unit="hour", tz="America/New_York"
    )
    fall = T.slice_range(
        "2026-11-01T00:00:00", "2026-11-02T00:00:00", unit="hour", tz="America/New_York"
    )
    assert len(spring) == 23
    assert len(fall) == 25


def test_ambiguous_wall_time_yields_both_instants():
    # 01:30 local occurs twice on 2026-11-01 in New York.
    candidates = T.wall_candidates(datetime(2026, 11, 1, 1, 30), "America/New_York")
    assert len(candidates) == 2
    assert candidates[1] - candidates[0] == timedelta(hours=1)


def test_nonexistent_wall_time_resolves_forward():
    # 02:30 local does not exist on 2026-03-08 in New York.
    assert T.wall_candidates(datetime(2026, 3, 8, 2, 30), "America/New_York") == []
    resolved = T.resolve_wall(datetime(2026, 3, 8, 2, 30), "America/New_York")
    assert len(resolved) == 1


def test_precision_rounds_outward_and_is_split_invariant():
    # 00:30 to 02:30 at day precision must become whole days.
    bounds = T.normalize_range("2026-11-01T00:30:00Z", "2026-11-03T02:30:00Z", precision="day")
    assert bounds == (
        datetime(2026, 11, 1, tzinfo=timezone.utc),
        datetime(2026, 11, 4, tzinfo=timezone.utc),
    )
    # Sub-hour precision still yields identical coverage for both units.
    hour = T.slice_range("2026-11-01T00:30:00Z", "2026-11-01T02:30:00Z", unit="hour", precision="minute")
    assert T.total_seconds(hour) == 2 * 3600


def test_precision_coarser_than_unit_is_rejected():
    with pytest.raises(ValueError):
        T.slice_range("2026-11-01", "2026-11-02", unit="hour", precision="day")


def test_naive_instants_are_interpreted_in_source_tz():
    assert T.parse_instant("2026-11-01T00:00:00", "America/New_York") == datetime(
        2026, 11, 1, 4, tzinfo=timezone.utc
    )
    with pytest.raises(ValueError):
        T.to_utc(datetime(2026, 11, 1))


def test_interval_algebra_is_half_open():
    a = T.interval("2026-11-01T00:00:00Z", "2026-11-01T02:00:00Z")
    b = T.interval("2026-11-01T02:00:00Z", "2026-11-01T04:00:00Z")
    assert a.intersection(b) is None  # touching, not overlapping
    assert T.merge_intervals([a, b]) == [T.interval("2026-11-01T00:00:00Z", "2026-11-01T04:00:00Z")]
    holes = T.subtract_intervals(
        [T.interval("2026-11-01T00:00:00Z", "2026-11-01T04:00:00Z")],
        [T.interval("2026-11-01T01:00:00Z", "2026-11-01T03:00:00Z")],
    )
    assert holes == [
        T.interval("2026-11-01T00:00:00Z", "2026-11-01T01:00:00Z"),
        T.interval("2026-11-01T03:00:00Z", "2026-11-01T04:00:00Z"),
    ]
