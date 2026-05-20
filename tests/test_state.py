from datetime import UTC, datetime

from datalake.utils.state import read_state, write_state


def test_state_round_trip(tmp_path):
    state_path = tmp_path / "state.json"
    written_at = datetime(2026, 5, 20, 12, 0, tzinfo=UTC)

    assert read_state(state_path) == {"last_success_utc": None}

    write_state(state_path, written_at, "run_123")
    state = read_state(state_path)

    assert state["last_success_utc"] == "2026-05-20T12:00:00+00:00"
    assert state["last_run_id"] == "run_123"
