"""Budget: reserve/settle/refund ledger, lane isolation, idempotent retries."""

from datetime import datetime, timedelta, timezone

import pytest

from src.timebox import budget as B

AT = datetime(2026, 11, 1, 12, tzinfo=timezone.utc)
NEXT_DAY = datetime(2026, 11, 2, 12, tzinfo=timezone.utc)


def ledger(capacity=10, reserve=0, tz="UTC", ttl=timedelta(hours=6)):
    return B.BudgetLedger(capacity, realtime_reserve=reserve, tz=tz, hold_ttl=ttl)


def test_reserve_settle_refund_are_individually_explainable():
    b = ledger()
    b.reserve("op1", 3, lane="backfill", at=AT)
    b.settle("op1", 2, at=AT)
    explained = b.explain("op1")
    kinds = [t["kind"] for t in explained["entry"]["txns"]]
    assert kinds == ["reserve", "settle"]
    assert explained["consumed"] == 2
    assert explained["outstanding"] == 0
    assert b.available("backfill", AT) == 8


def test_reserving_same_op_twice_does_not_double_charge():
    b = ledger()
    _, created_first = b.reserve("op", 2, at=AT)
    _, created_second = b.reserve("op", 2, at=AT)
    assert created_first is True
    assert created_second is False
    assert b.available("backfill", AT) == 8
    b.settle("op", 2, at=AT)
    b.settle("op", 2, at=AT)  # replay
    assert b.available("backfill", AT) == 8
    assert len(b.transactions()) == 2


def test_conflicting_settlement_is_rejected_not_silently_applied():
    b = ledger()
    b.reserve("op", 3, at=AT)
    b.settle("op", 3, at=AT)
    with pytest.raises(B.ConflictingSettlement):
        b.settle("op", 2, at=AT)


def test_refunded_operation_cannot_be_settled():
    b = ledger()
    b.reserve("op", 3, at=AT)
    b.refund("op", reason="client_error", at=AT)
    with pytest.raises(B.ConflictingSettlement):
        b.settle("op", 1, at=AT)


def test_refund_is_idempotent_and_restores_budget():
    b = ledger()
    b.reserve("op", 4, at=AT)
    assert b.available("backfill", AT) == 6
    b.refund("op", at=AT)
    b.refund("op", at=AT)
    assert b.available("backfill", AT) == 10
    assert len(b.transactions()) == 2


def test_unknown_outcome_keeps_hold_and_retry_reuses_it():
    b = ledger()
    for _ in range(5):
        b.reserve("op", 1, lane="backfill", at=AT)
        b.mark_unknown("op", at=AT)
    kinds = [t["kind"] for t in b.transactions()]
    assert kinds == ["reserve", "unknown"]
    assert b.available("backfill", AT) == 9
    assert b.snapshot(AT)["held"] == 1


def test_unknown_hold_expires_so_realtime_is_never_blocked_forever():
    b = ledger(ttl=timedelta(minutes=30))
    b.reserve("op", 4, at=AT)
    b.mark_unknown("op", at=AT)
    assert b.available("realtime", AT) == 6
    released = b.expire(AT + timedelta(hours=1))
    assert released == ["op"]
    assert b.available("realtime", AT + timedelta(hours=1)) == 10
    assert b.get("op").txns[-1]["reason"] == "unknown_expired"


def test_backfill_cannot_starve_realtime():
    b = ledger(capacity=10, reserve=4)
    assert b.available("backfill", AT) == 6
    assert b.available("realtime", AT) == 10
    b.reserve("bf", 6, lane="backfill", at=AT)
    with pytest.raises(B.BudgetExceeded):
        b.reserve("bf2", 1, lane="backfill", at=AT)
    # Backfill has spent its whole share; the realtime reserve (4) is intact
    # and still reservable in full.
    assert b.available("realtime", AT) == 4
    b.reserve("rt", 4, lane="realtime", at=AT)
    assert b.available("realtime", AT) == 0


def test_settlement_overage_is_recorded_and_charged_to_its_lane():
    b = ledger(capacity=10, reserve=3)
    b.reserve("bf", 1, lane="backfill", at=AT)
    b.settle("bf", 3, at=AT)
    tx = b.get("bf").txns[-1]
    assert tx["overage"] == 2
    assert b.available("backfill", AT) == 4
    assert b.available("realtime", AT) == 7  # overage never eats the realtime reserve


def test_cross_day_settlement_charges_the_original_period():
    b = ledger(capacity=5)
    b.reserve("op", 2, at=AT)
    b.settle("op", 2, at=NEXT_DAY)  # settles after local midnight
    assert b.get("op").period == "2026-11-01"
    assert b.snapshot(AT)["used"] == 2
    assert b.snapshot(NEXT_DAY)["used"] == 0
    assert b.available("backfill", NEXT_DAY) == 5


def test_period_rollover_gives_fresh_budget():
    b = ledger(capacity=2)
    b.reserve("d1", 2, at=AT)
    b.settle("d1", 2, at=AT)
    assert b.available("backfill", AT) == 0
    assert b.available("backfill", NEXT_DAY) == 2


def test_budget_period_uses_configured_timezone():
    # 2026-11-02T02:00Z is still 2026-11-01 in New York (UTC-4).
    early_utc = datetime(2026, 11, 2, 2, tzinfo=timezone.utc)
    b = ledger(tz="America/New_York")
    assert b.period_of(early_utc) == "2026-11-01"
    assert b.period_of(datetime(2026, 11, 2, 6, tzinfo=timezone.utc)) == "2026-11-02"


def test_dump_and_load_preserve_ledger_and_pending_holds():
    b = ledger()
    b.reserve("held", 2, at=AT)
    b.reserve("done", 1, at=AT)
    b.settle("done", 1, at=AT)
    restored = B.BudgetLedger.load(b.dump())
    assert restored.available("backfill", AT) == b.available("backfill", AT)
    assert restored.get("held").state == "held"
    assert restored.get("done").actual == 1
