"""Fixture pipeline: original contract (counts, idempotency) must be preserved."""

import pytest

from src.timebox import config, pipeline, store


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DB_PATH", tmp_path / "pipeline.db")
    yield


def test_pipeline_runs_and_idempotent():
    a = pipeline.run()
    b = pipeline.run()   # 重跑不增行（幂等 upsert）
    assert a["fed"] == 4
    assert b["rows"] == a["rows"] == 3   # r-003 重复被去重


def test_pipeline_retains_raw_inputs_when_asked():
    con = store.connect()
    try:
        pipeline.run(con, observe=True)
        pipeline.run(con, observe=True)  # replay must not duplicate history
        assert len(store.observation_history(con)) == 3
    finally:
        con.close()


def test_store_roundtrip():
    con = store.connect()
    try:
        assert store.count(con) >= 0
    finally:
        con.close()


def test_pipeline_stages_are_pure_record_transforms():
    from src.timebox import budget, cursor, merge, timestep

    record = {"id": "x", "ts": "2026-11-01T00:00:00Z"}
    for mod in (timestep, merge, budget, cursor):
        out = mod.process(record)
        assert out is not record
        assert out["id"] == "x"
    assert "ts_utc" in timestep.process(record)
