"""Deterministic tests for the two-scrape selection rules."""

import time

from app.histogram import HistogramSnapshot
from app.scraper import FRESHNESS_WINDOW, MIN_SAMPLE_GAP, ScrapeRecord, ScrapeStore


def _record(at: float, pid: str, buckets=(0.1, 0.5, 1.0), counts=(1, 2, 3),
            total=4, ok=True, error=None, table_id=None, previous=None) -> ScrapeRecord:
    if not ok:
        return ScrapeRecord(ok=False, at=at, error=error)
    snap = HistogramSnapshot(
        metric_name="m",
        buckets=dict(zip(buckets, counts)),
        count=total,
        sum_value=1.0,
        process_id=pid,
        scrape_seq=None,
        scraped_at=at,
        table_id=table_id,
        previous=previous,
    )
    return ScrapeRecord(ok=True, at=at, snapshot=snap)


def _store(records):
    store = ScrapeStore({"i1": "http://x/metrics"})
    store.instances["i1"].history = records
    return store


def test_select_happy_path():
    now = 100.0
    store = _store([_record(now - 2.0, "p1"), _record(now - 0.9, "p1",
                                                       counts=(2, 4, 6), total=8)])
    outcome = store._select_sample(store.instances["i1"], now)
    assert not isinstance(outcome, str)
    assert outcome.delta.total_requests == 4
    assert outcome.sample_times == (now - 2.0, now - 0.9)


def test_select_insufficient_scrapes():
    store = _store([_record(1.0, "p1")])
    assert store._select_sample(store.instances["i1"], 2.0) == "insufficient_scrapes"


def test_select_ignores_failed_scrapes():
    now = 10.0
    store = _store([
        _record(now - 2.1, "p1"),
        _record(now - 1.5, "x", ok=False, error="boom"),
        _record(now - 1.0, "p1", counts=(2, 4, 6), total=8),
    ])
    outcome = store._select_sample(store.instances["i1"], now)
    # The two successful records are 1.1s apart and both fresh.
    assert not isinstance(outcome, str)
    assert outcome.delta.total_requests == 4


def test_select_stale_scrape():
    now = time.time()
    store = _store([
        _record(now - FRESHNESS_WINDOW - 0.1, "p1"),
        _record(now - FRESHNESS_WINDOW - 0.05, "p1"),
    ])
    assert store._select_sample(store.instances["i1"], now) == "stale_scrape"


def test_select_samples_too_close():
    now = time.time()
    store = _store([
        _record(now - 0.9, "p1"),
        _record(now - 0.05, "p1"),
    ])
    assert store._select_sample(store.instances["i1"], now) == "samples_too_close"


def test_select_process_restarted():
    now = time.time()
    store = _store([
        _record(now - 2.0, "p1"),
        _record(now - 0.9, "p2"),
    ])
    assert store._select_sample(store.instances["i1"], now) == "process_restarted"


def test_select_counter_reset_treated_as_restart():
    now = time.time()
    # Same process label but the newer counters are lower -> reset.
    store = _store([
        _record(now - 2.0, "p1", counts=(5, 9, 10), total=10),
        _record(now - 0.9, "p1", counts=(1, 2, 3), total=4),
    ])
    assert store._select_sample(store.instances["i1"], now) == "process_restarted"


def test_gap_constant_matches_requirement():
    assert MIN_SAMPLE_GAP >= 1.0
    assert FRESHNESS_WINDOW <= 3.0


# --------------------------------------------------------------------------- #
# Bucket-table switches
# --------------------------------------------------------------------------- #


def test_select_bridges_a_table_switch_between_the_two_scrapes():
    now = time.time()
    retro = HistogramSnapshot(
        metric_name="m",
        buckets={0.1: 4, 0.5: 6, 1.0: 8},
        count=8,
        sum_value=1.0,
        process_id="p1",
        scrape_seq=None,
        scraped_at=now - 0.5,
        table_id="t1",
    )
    store = _store([
        _record(now - 2.0, "p1", buckets=(0.1, 0.5, 1.0), counts=(2, 4, 6),
                total=6, table_id="t1"),
        _record(now - 0.5, "p1", buckets=(0.2, 1.0), counts=(3, 5),
                total=5, table_id="t2", previous=retro),
    ])
    outcome = store._select_sample(store.instances["i1"], now)
    assert not isinstance(outcome, str)
    assert outcome.table_ids == ["t1", "t2"]
    assert outcome.crosses_table_change
    # old table: 8 - 6 = 2 requests; new table: all 5 of its counters
    assert outcome.delta.total_requests == 7


def test_select_reports_missing_bridge_as_bucket_table_changed():
    now = time.time()
    store = _store([
        _record(now - 2.0, "p1", buckets=(0.1, 0.5, 1.0), counts=(2, 4, 6),
                total=6, table_id="t1"),
        _record(now - 0.5, "p1", buckets=(0.2, 1.0), counts=(3, 5),
                total=5, table_id="t2"),
    ])
    assert (
        store._select_sample(store.instances["i1"], now)
        == "bucket_table_changed"
    )


def test_select_ignores_unrelated_retro_table():
    now = time.time()
    # A retro table from an older generation is exposed, but both scrapes
    # use the same current table: the delta is a plain one-segment delta.
    retro = HistogramSnapshot(
        metric_name="m",
        buckets={0.1: 99, 0.5: 99, 1.0: 99},
        count=99,
        sum_value=1.0,
        process_id="p1",
        scrape_seq=None,
        scraped_at=now - 0.5,
        table_id="t0",
    )
    store = _store([
        _record(now - 2.0, "p1", counts=(1, 2, 3), total=4, table_id="t1"),
        _record(now - 0.5, "p1", counts=(2, 4, 6), total=8, table_id="t1",
                previous=retro),
    ])
    outcome = store._select_sample(store.instances["i1"], now)
    assert not isinstance(outcome, str)
    assert not outcome.crosses_table_change
    assert outcome.delta.total_requests == 4


def test_switch_gate_blocks_until_the_new_table_is_received():
    """A further switch waits until a scrape shows the submitted table."""

    now = time.time()
    new_boundaries = [0.2, 0.6, 1.2]
    store = _store([_record(now - 1.0, "p1", table_id="srv-t1")])
    assert store.switch_block_reason("i1") is None

    # Simulate a submission that has not been reflected in a scrape yet.
    store.instances["i1"].submitted_table = "pending"
    store.instances["i1"].submitted_from_table = "srv-t1"
    store.instances["i1"].submitted_boundaries = new_boundaries
    assert store.switch_block_reason("i1") is not None

    # An unrelated table (e.g. from a restarted target) does not satisfy the
    # gate, even though the table id differs.
    store.instances["i1"].history.append(
        _record(now - 0.2, "p1", buckets=(0.05, 0.5, 1.0), table_id="srv-other")
    )
    assert store.switch_block_reason("i1") is not None

    # The next scrape carries the submitted boundaries: the gate opens.
    store.instances["i1"].history.append(
        _record(now - 0.1, "p1", buckets=tuple(new_boundaries), table_id="srv-t2")
    )
    assert store.switch_block_reason("i1") is None
    assert store.instances["i1"].submitted_table == "srv-t2"
    assert store.instances["i1"].confirmed_table_id == "srv-t2"


def test_switch_gate_reopens_if_the_sample_restarted_before_receipt():
    now = time.time()
    store = _store([_record(now - 1.0, "p1", table_id="srv-t1")])
    store.instances["i1"].submitted_table = "pending"
    store.instances["i1"].submitted_from_table = "srv-t1"
    store.instances["i1"].submitted_boundaries = [0.2, 0.6, 1.2]
    store.instances["i1"].submitted_process_id = "p1"

    # A restarted process that never shows the submitted table means the
    # submission was lost; the gate must reopen instead of wedging forever.
    store.instances["i1"].history.append(
        _record(now - 0.1, "p2", table_id="srv-t1")
    )
    assert store.switch_block_reason("i1") is None
    assert store.instances["i1"].submitted_table is None
