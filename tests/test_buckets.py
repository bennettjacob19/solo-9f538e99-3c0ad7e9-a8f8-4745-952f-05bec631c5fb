"""Unit tests for bucket-table validation and cross-table deltas."""

import math

import pytest

from app.buckets import BucketValidationError, validate_boundaries
from app.histogram import (
    INF,
    BucketTransitionError,
    HistogramSnapshot,
    combine_instances,
    compliant_count_bounds,
    compute_delta,
    parse_metrics_text,
    segment_bounds,
)


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "values, expected",
    [
        ([0.1, 0.2, 1.0], [0.1, 0.2, 1.0]),
        (["0.5", "2"], [0.5, 2.0]),
        ((0.001,), [0.001]),
    ],
)
def test_validate_accepts_strictly_increasing_positive_finite(values, expected):
    assert validate_boundaries(values) == expected


@pytest.mark.parametrize(
    "values",
    [
        [],
        [0.1, 0.1],
        [0.2, 0.1],
        [0.0, 0.1],
        [-0.1, 0.3],
        [0.1, math.inf],
        [math.nan, 0.1],
        [0.1, math.nan],
        [math.inf],
    ],
)
def test_validate_rejects_invalid_tables(values):
    with pytest.raises(BucketValidationError):
        validate_boundaries(values)


def test_validate_rejects_non_numeric():
    with pytest.raises(BucketValidationError):
        validate_boundaries(["abc"])


# --------------------------------------------------------------------------- #
# Multi-table exposition
# --------------------------------------------------------------------------- #

CURRENT_AND_RETRO = """# TYPE http_request_duration_seconds histogram
http_request_duration_seconds_bucket{le="0.2",run_id="r1",bucket_table="t2"} 7
http_request_duration_seconds_bucket{le="1",run_id="r1",bucket_table="t2"} 9
http_request_duration_seconds_bucket{le="+Inf",run_id="r1",bucket_table="t2"} 10
http_request_duration_seconds_count{run_id="r1",bucket_table="t2"} 10
http_request_duration_seconds_sum{run_id="r1",bucket_table="t2"} 3
http_request_duration_seconds_bucket{le="0.1",run_id="r1",bucket_table="t1",retro="true"} 14
http_request_duration_seconds_bucket{le="0.5",run_id="r1",bucket_table="t1",retro="true"} 24
http_request_duration_seconds_bucket{le="+Inf",run_id="r1",bucket_table="t1",retro="true"} 29
http_request_duration_seconds_count{run_id="r1",bucket_table="t1",retro="true"} 29
http_request_duration_seconds_sum{run_id="r1",bucket_table="t1",retro="true"} 8
process_start_time_seconds 1.7e9
"""


def _snap(boundaries, cumulative, count, sum_value=0.0, pid="p1", at=0.0,
          table_id=None, previous=None):
    return HistogramSnapshot(
        metric_name="m",
        buckets=dict(zip(boundaries, cumulative)),
        count=count,
        sum_value=sum_value,
        process_id=pid,
        scrape_seq=None,
        scraped_at=at,
        table_id=table_id,
        previous=previous,
    )


def test_parse_current_and_retro_tables():
    snapshot = parse_metrics_text(
        CURRENT_AND_RETRO, "http_request_duration_seconds", 10.0
    )
    assert snapshot.table_id == "t2"
    assert snapshot.count == 10
    assert snapshot.buckets == {0.2: 7, 1.0: 9, INF: 10}
    assert snapshot.previous is not None
    assert snapshot.previous.table_id == "t1"
    assert snapshot.previous.count == 29
    assert snapshot.previous.buckets == {0.1: 14, 0.5: 24, INF: 29}
    assert snapshot.process_id == "r1"


def test_parse_without_table_label_keeps_behaviour():
    text = """http_request_duration_seconds_bucket{le="0.1",run_id="r1"} 5
http_request_duration_seconds_bucket{le="+Inf",run_id="r1"} 6
http_request_duration_seconds_count{run_id="r1"} 6
"""
    snapshot = parse_metrics_text(text, "http_request_duration_seconds", 0.0)
    assert snapshot.table_id is None
    assert snapshot.previous is None
    assert snapshot.count == 6


# --------------------------------------------------------------------------- #
# Segmented deltas
# --------------------------------------------------------------------------- #


def test_delta_across_table_change_splits_windows():
    earlier = _snap([0.1, 0.5], [10, 20], 25, table_id="t1")
    retro = _snap([0.1, 0.5], [14, 24], 29, table_id="t1", at=2.0)
    later = _snap(
        [0.2, 1.0], [7, 9], 10, table_id="t2", at=2.0, previous=retro
    )
    delta = compute_delta(earlier, later)

    assert delta.crosses_table_change
    assert delta.table_ids == ["t1", "t2"]
    old, new = delta.segments
    # old table: requests observed after ``earlier`` until the switch
    assert old.delta_buckets == [4, 4]
    assert old.delta_inf == 4
    assert old.from_zero_baseline is False
    # new table: all of its counts happened inside the window
    assert new.delta_buckets == [7, 9]
    assert new.delta_inf == 10
    assert new.from_zero_baseline is True
    assert delta.total_requests == 14


def test_delta_across_table_change_counts_each_request_once():
    # old segment 4 requests + new segment 10 = 14 total; the new table's
    # counters include *only* traffic after the switch.
    earlier = _snap([0.1, 0.5], [10, 20], 25, table_id="t1")
    retro = _snap([0.1, 0.5], [14, 24], 29, table_id="t1", at=2.0)
    later = _snap([0.2, 1.0], [7, 9], 10, table_id="t2", at=2.0, previous=retro)
    delta = compute_delta(earlier, later)
    assert delta.total_requests == 14
    assert sum(segment.delta_inf for segment in delta.segments) == 14


def test_delta_table_change_without_bridge_is_rejected():
    earlier = _snap([0.1, 0.5], [10, 20], 25, table_id="t1")
    later = _snap([0.2, 1.0], [7, 9], 10, table_id="t2", at=2.0)
    with pytest.raises(BucketTransitionError):
        compute_delta(earlier, later)


def test_delta_rejects_mismatched_bridge_layout():
    earlier = _snap([0.1, 0.5], [10, 20], 25, table_id="t1")
    retro = _snap([0.1, 0.3], [14, 24], 29, table_id="t1", at=2.0)
    later = _snap([0.2, 1.0], [7, 9], 10, table_id="t2", at=2.0, previous=retro)
    with pytest.raises(BucketTransitionError):
        compute_delta(earlier, later)


def test_delta_ignores_retro_table_when_no_switch_happened():
    retro = _snap([0.1, 0.5], [99, 99], 99, table_id="t0", at=1.0)
    earlier = _snap([0.1, 0.5], [10, 20], 25, table_id="t1", at=0.0)
    later = _snap([0.1, 0.5], [15, 22], 30, table_id="t1", at=1.1, previous=retro)
    delta = compute_delta(earlier, later)
    assert not delta.crosses_table_change
    assert delta.segments[0].delta_inf == 5


# --------------------------------------------------------------------------- #
# Segmented bounds
# --------------------------------------------------------------------------- #


def _switched_delta():
    earlier = _snap([0.1, 0.5], [10, 20], 25, table_id="t1")
    # old table gained 2 requests <= 0.1, 3 more <= 0.5 (4 total)
    retro = _snap([0.1, 0.5], [12, 23], 29, table_id="t1", at=2.0)
    # new table: 6 <= 0.2, 8 <= 0.7, 9 <= 2.0, 10 total
    later = _snap(
        [0.2, 0.7, 2.0], [6, 8, 9], 10, table_id="t2", at=2.0, previous=retro
    )
    return compute_delta(earlier, later)


def test_bounds_use_each_table_for_its_own_segment():
    delta = _switched_delta()
    old, new = delta.segments

    # threshold 0.5 sits on an old-table boundary: exactly 3 old requests.
    assert segment_bounds(old, 0.5).lower == 3
    assert segment_bounds(old, 0.5).upper == 3
    # threshold 0.5 cuts the new table's bucket (0.2, 0.7]: 6..8.
    assert segment_bounds(new, 0.5).lower == 6
    assert segment_bounds(new, 0.5).upper == 8

    merged = compliant_count_bounds(delta, 0.5)
    assert merged.lower == 9   # 3 + 6
    assert merged.upper == 11  # 3 + 8
    assert delta.total_requests == 14


def test_bounds_threshold_exact_on_one_table_can_be_uncertain_on_other():
    delta = _switched_delta()
    # 0.2 is exactly a new-table boundary (6 known) but sits inside the old
    # table's bucket (0.1, 0.5] (2 known, up to 3). -> 8..9
    bounds = compliant_count_bounds(delta, 0.2)
    assert bounds.lower == 8
    assert bounds.upper == 9
    # 0.1 sits exactly on an old-table boundary (2 known) but below the new
    # table's smallest boundary, so the new segment is 0..6. -> 2..8
    bounds = compliant_count_bounds(delta, 0.1)
    assert bounds.lower == 2
    assert bounds.upper == 8


def test_combine_instances_with_one_instance_crossing_a_switch():
    switched = _switched_delta()
    steady = compute_delta(
        _snap([0.1, 0.5], [0, 0], 0, table_id="x"),
        _snap([0.1, 0.5], [10, 18], 20, table_id="x", at=1.1),
    )
    combined = combine_instances({"a": switched, "b": steady}, threshold_s=0.5)
    # switched: 9..11 of 14; steady: exactly 18 of 20
    assert combined.per_instance["a"].lower == 9
    assert combined.per_instance["a"].upper == 11
    assert combined.per_instance["b"].lower == 18
    assert combined.per_instance["b"].upper == 18
    assert combined.total_requests == 34
    assert combined.compliant_lower == 27
    assert combined.compliant_upper == 29
