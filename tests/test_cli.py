"""CLI: fixture ingest unchanged, backfill/status commands queryable."""

import json

import pytest

from src.timebox import cli, config


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DB_PATH", tmp_path / "cli.db")
    yield


def run_cli(capsys, *argv):
    code = cli.main(list(argv))
    out = capsys.readouterr().out
    return code, (json.loads(out) if out.strip() else None)


def test_default_command_keeps_fixture_contract(capsys):
    code, payload = run_cli(capsys)
    assert code == 0
    assert payload == {"fed": 4, "rows": 3}


def test_backfill_then_status_reports_coverage(capsys):
    code, payload = run_cli(capsys, "backfill", "--start", "2026-11-01", "--end", "2026-11-04")
    assert code == 0
    assert payload["run"]["done"] is True
    assert payload["status"]["coverage"]["complete"] is True
    assert sorted(payload["status"]["results"]) == ["r-001", "r-002", "r-003"]

    code, status = run_cli(capsys, "status", "--start", "2026-11-01", "--end", "2026-11-04")
    assert code == 0
    assert status["coverage"]["pending"] == []
    assert status["resume_position"] is None


def test_rerunning_backfill_fetches_nothing(capsys):
    run_cli(capsys, "backfill", "--start", "2026-11-01", "--end", "2026-11-04")
    code, payload = run_cli(capsys, "backfill", "--start", "2026-11-01", "--end", "2026-11-04")
    assert code == 0
    assert payload["run"]["slices"] == []
    assert payload["status"]["coverage"]["complete"] is True


def test_partial_range_reports_resume_position(capsys):
    code, payload = run_cli(capsys, "backfill", "--start", "2026-11-01", "--end", "2026-11-03")
    assert code == 0
    full = run_cli(capsys, "status", "--start", "2026-11-01", "--end", "2026-11-05")[1]
    assert full["coverage"]["complete"] is False
    assert full["resume_position"] == "2026-11-03T00:00:00.000000Z"
