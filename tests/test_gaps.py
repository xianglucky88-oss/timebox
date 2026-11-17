import pytest

def test_gapfill_is_open_gap():
    from src.timebox import gapfill
    with pytest.raises(NotImplementedError):
        gapfill.process({"id": "x"})

def test_backfill_is_open_gap():
    from src.timebox import backfill
    with pytest.raises(NotImplementedError):
        backfill.process({"id": "x"})
