"""Direct tests of the sample target's bucket-switch state machine."""

import math

import pytest
from fastapi import HTTPException

from app.buckets import BucketValidationError
from app.sample_target import BucketTableCounts, TargetState


def _state(boundaries=(0.1, 0.5, 1.0)) -> TargetState:
    return TargetState("sample-test", list(boundaries))


def test_initial_table_and_reset_semantics():
    state = _state()
    first = state.current.table_id
    state.inject(0.2, 3)
    assert state.current.total_count == 3

    state.reset()  # simulated restart
    assert state.current.table_id == first  # table id survives a restart
    assert state.current.boundaries == [0.1, 0.5, 1.0]
    assert state.current.total_count == 0
    assert state.retro is None


def test_injection_goes_to_the_active_table_only():
    state = _state()
    state.inject(0.2, 4)
    state.request_switch([0.2, 0.4])
    # Still pending: the injection must land in the old table.
    state.inject(0.2, 1)
    assert state.current.total_count == 5
    state.render_metrics()  # applies the pending switch
    state.inject(0.2, 2)
    assert state.current.total_count == 2
    assert state.retro is not None and state.retro.total_count == 5
    # Every request counted exactly once, across both tables.
    assert state.current.total_count + state.retro.total_count == 7


def test_only_one_retro_table_is_retained():
    state = _state()
    state.inject(0.2, 10)
    state.request_switch([0.2, 0.4])
    state.render_metrics()
    first_table = state.retro.table_id
    state.inject(0.2, 20)
    state.request_switch([0.05, 0.3])
    state.render_metrics()
    # The old retro was replaced: only the immediately preceding table stays.
    assert state.retro is not None
    assert state.retro.table_id != first_table
    assert state.retro.total_count == 20

    text = state.render_metrics()
    retro_series = [line for line in text.splitlines() if 'retro="true"' in line]
    retro_ids = {line.split('bucket_table="')[1].split('"')[0] for line in retro_series}
    assert retro_ids == {state.retro.table_id}


def test_second_switch_is_refused_while_one_is_pending():
    state = _state()
    state.request_switch([0.2, 0.4])
    with pytest.raises(HTTPException) as exc:
        state.request_switch([0.3, 0.6])
    assert exc.value.status_code == 409
    assert state.pending_boundaries == [0.2, 0.4]


def test_invalid_switch_keeps_the_current_table():
    state = _state()
    before = state.current.table_id
    for bad in ([], [0.5, 0.5], [0.5, 0.2], [0.0], [-1.0], [math.inf], [math.nan]):
        with pytest.raises(BucketValidationError):
            state.request_switch(bad)
    assert state.pending_boundaries is None
    assert state.current.table_id == before
    assert state.switch_count == 0


def test_switch_survives_a_simulated_restart_and_applies_on_next_scrape():
    state = _state()
    state.inject(0.2, 6)
    state.request_switch([0.2, 0.4])
    state.reset()
    assert state.pending_boundaries == [0.2, 0.4]
    state.render_metrics()
    assert state.current.boundaries == [0.2, 0.4]
    assert state.current.total_count == 0
    # The bridge covers the counters accumulated *since* the restart, so the
    # frozen old-table counts are all zero (never the pre-restart ones).
    assert state.retro is not None and state.retro.total_count == 0


def test_requiring_a_switch_at_the_next_scrape_not_immediately():
    state = _state()
    before = state.current.table_id
    state.request_switch([0.2, 0.4])
    assert state.current.table_id == before  # nothing changed yet
    assert state.switch_status()["status"] == "pending"
    state.render_metrics()
    assert state.current.table_id != before
    assert state.switch_status()["status"] == "idle"
    assert state.switch_status()["retro_table"]["table_id"] == before
