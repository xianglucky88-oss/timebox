"""Store: idempotent records, retained input history, durable engine state."""

import pytest

from src.timebox import store
from src.timebox.models import Record


@pytest.fixture()
def con(tmp_path):
    connection = store.connect(tmp_path / "test.db")
    try:
        yield connection
    finally:
        connection.close()


def test_upsert_is_idempotent_and_overwrites_current_view(con):
    store.upsert(con, Record(key="k", data={"v": 1}))
    store.upsert(con, Record(key="k", data={"v": 2}))
    store.commit(con)
    assert store.count(con) == 1
    assert store.get_record(con, "k") == {"v": 2}


def test_old_input_is_retained_when_current_view_changes(con):
    store.upsert(con, Record(key="k", data={"v": 1}))
    store.record_observation(con, "k", {"id": "k", "v": 1})
    store.upsert(con, Record(key="k", data={"v": 2}))
    store.record_observation(con, "k", {"id": "k", "v": 2})
    store.commit(con)
    history = store.observation_history(con, "k")
    assert [h["data"]["v"] for h in history] == [1, 2]


def test_identical_input_history_is_deduplicated(con):
    for _ in range(3):
        store.record_observation(con, "k", {"id": "k", "v": 1})
    store.commit(con)
    assert len(store.observation_history(con)) == 1


def test_engine_state_round_trips(con):
    store.save_state(con, "fixture", {"slices": [1, 2, 3]})
    store.commit(con)
    assert store.load_state(con, "fixture") == {"slices": [1, 2, 3]}
    store.save_state(con, "fixture", {"slices": []})
    store.commit(con)
    assert store.load_state(con, "fixture") == {"slices": []}
    assert store.load_state(con, "missing") is None
