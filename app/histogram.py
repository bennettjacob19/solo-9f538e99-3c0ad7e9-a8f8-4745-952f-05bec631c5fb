"""Core logic for the canary rollout SLO check.

This module only contains pure functions / data classes so it can be unit
tested without any HTTP or async machinery.

Prometheus histograms are *cumulative*: every ``_bucket{le="x"}`` counts the
observations whose value is <= x, and ``+Inf`` equals ``_count``.  Between two
scrapes of the same process, the number of requests that landed in a bucket
range is therefore the difference of two cumulative counts.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field

INF = float("inf")
# Internal label used by the bundled sample targets to identify a process.
PROCESS_LABEL = "run_id"
# Internal label identifying the bucket table a sample was counted under.
TABLE_LABEL = "bucket_table"


class MetricsParseError(ValueError):
    """Raised when exposition text cannot be interpreted as expected."""


class BucketTransitionError(MetricsParseError):
    """A bucket-table change happened and cannot be bridged with the data."""


@dataclass(frozen=True)
class HistogramSnapshot:
    """One scraped histogram (cumulative bucket counters).

    ``table_id`` identifies the bucket table the counters belong to.  When a
    target has switched tables, ``previous`` carries the frozen counts of the
    immediately preceding table (at most one retro table is exposed), which
    lets the verifier split a scrape window that straddles the switch into an
    old-table segment and a new-table segment.
    """

    metric_name: str
    buckets: dict[float, int]  # ordered upper bound -> cumulative count
    count: int
    sum_value: float
    process_id: str | None
    scrape_seq: int | None
    scraped_at: float  # wall-clock seconds (time.time())
    table_id: str | None = None
    previous: "HistogramSnapshot | None" = None

    def __post_init__(self) -> None:
        if INF not in self.buckets:
            object.__setattr__(self, "buckets", {**self.buckets, INF: self.count})


@dataclass
class HistogramSegmentDelta:
    """Incremental counts for one bucket table inside a scrape window.

    A window that straddles a bucket-table change is split into one segment
    per table, so that every request is counted exactly once and always under
    the table that was active when it was observed.
    """

    table_id: str | None
    boundaries: list[float]  # finite upper bounds, sorted ascending
    delta_buckets: list[int]  # cumulative counts of window requests
    delta_inf: int  # requests counted in this segment
    delta_sum: float
    earlier: HistogramSnapshot
    later: HistogramSnapshot
    # True when ``earlier`` is the synthetic all-zero baseline of a freshly
    # created table (new tables start at zero when the switch is applied).
    from_zero_baseline: bool = False

    @property
    def total_requests(self) -> int:
        return self.delta_inf


@dataclass
class HistogramDelta:
    """Incremental bucket counts between two snapshots of one process.

    Normally there is exactly one segment.  If the target switched its bucket
    table between the two scrapes, ``segments`` holds the old-table segment
    followed by the new-table segment.
    """

    metric_name: str
    segments: list[HistogramSegmentDelta]
    earlier: HistogramSnapshot
    later: HistogramSnapshot

    @property
    def boundaries(self) -> list[float]:
        """Boundaries of the last segment (compatibility helper)."""
        return self.segments[-1].boundaries

    @property
    def delta_buckets(self) -> list[int]:
        """Bucket increments of the last segment (compatibility helper)."""
        return self.segments[-1].delta_buckets

    @property
    def delta_inf(self) -> int:
        return sum(segment.delta_inf for segment in self.segments)

    @property
    def delta_sum(self) -> float:
        return sum(segment.delta_sum for segment in self.segments)

    @property
    def total_requests(self) -> int:
        return self.delta_inf

    @property
    def table_ids(self) -> list[str | None]:
        return [segment.table_id for segment in self.segments]

    @property
    def crosses_table_change(self) -> bool:
        return len(self.segments) > 1


# --------------------------------------------------------------------------- #
# Parsing
# --------------------------------------------------------------------------- #

_LINE_RE = re.compile(r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?P<labels>\{.*\})?\s+(?P<value>\S+)\s*$")
_LABEL_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*"((?:\\.|[^"\\])*)"')


@dataclass
class _ParsedSeries:
    """One bucket table found in an exposition (current or retro)."""

    table_id: str | None
    retro: bool
    buckets: dict[float, int] = field(default_factory=dict)
    count: int | None = None
    sum_value: float = 0.0


def _parse_labels(raw: str | None) -> dict[str, str]:
    if not raw:
        return {}
    return {m.group(1): _unescape(m.group(2)) for m in _LABEL_RE.finditer(raw)}


def _unescape(value: str) -> str:
    return re.sub(r"\\(.)", r"\1", value)


def _parse_le(raw: str) -> float:
    if raw in ("+Inf", "-Inf", "Inf"):
        return INF
    return float(raw)


def parse_metrics_text(
    text: str,
    metric_name: str,
    scraped_at: float,
) -> HistogramSnapshot:
    """Parse Prometheus exposition text for one histogram metric.

    Expects ``<metric_name>_bucket{...}``, ``<metric_name>_count`` and
    ``<metric_name>_sum`` samples plus the process identity labels
    ``process_start_time_seconds`` / ``sample_run_id``.

    Bucket tables are described by an optional ``bucket_table`` label.  The
    target may additionally expose the frozen counters of the immediately
    preceding table on the same metric, marked with ``retro="true"``; those
    become :attr:`HistogramSnapshot.previous` so a scrape window that spans a
    table change can still be split into an old-table and a new-table segment.
    """

    series: dict[tuple[str | None, bool], _ParsedSeries] = {}
    process_start: float | None = None
    scrape_seq: int | None = None
    process_id: str | None = None

    def _series(labels: dict[str, str]) -> _ParsedSeries:
        table_id = labels.get(TABLE_LABEL)
        retro = labels.get("retro", "").strip().lower() in ("1", "true", "yes")
        key = (table_id, retro)
        if key not in series:
            series[key] = _ParsedSeries(table_id=table_id, retro=retro)
        return series[key]

    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = _LINE_RE.match(line)
        if not match:
            continue
        name = match.group("name")
        labels = _parse_labels(match.group("labels"))
        value_raw = match.group("value")

        if name == "process_start_time_seconds":
            process_start = float(value_raw)
            continue
        if name == "sample_scrape_seq":
            scrape_seq = int(float(value_raw))
            continue

        if name == f"{metric_name}_bucket":
            if "le" not in labels:
                continue
            target_series = _series(labels)
            target_series.buckets[_parse_le(labels["le"])] = int(float(value_raw))
            if PROCESS_LABEL in labels and process_id is None:
                process_id = labels[PROCESS_LABEL]
        elif name == f"{metric_name}_count":
            _series(labels).count = int(float(value_raw))
        elif name == f"{metric_name}_sum":
            _series(labels).sum_value = float(value_raw)

    usable = [s for s in series.values() if s.buckets]
    if not usable:
        raise MetricsParseError(f"no buckets found for metric {metric_name!r}")

    current_series = [s for s in usable if not s.retro]
    if not current_series:
        raise MetricsParseError(
            f"only retro bucket tables found for metric {metric_name!r}"
        )
    # Normally exactly one current table; keep selection deterministic.
    current = current_series[0]

    # Process identity: prefer the run_id label on histogram samples; fall
    # back to the process_start_time_seconds gauge when the label is absent.
    if process_id is None and process_start is not None:
        process_id = f"start-{process_start:f}"

    def _build(item: _ParsedSeries) -> HistogramSnapshot:
        buckets = item.buckets
        count = item.count
        if count is None:
            count = buckets.get(INF)
        if count is None:
            raise MetricsParseError(
                f"no {metric_name}_count sample and no +Inf bucket"
            )
        ordered = dict(sorted(buckets.items(), key=lambda pair: pair[0]))
        return HistogramSnapshot(
            metric_name=metric_name,
            buckets=ordered,
            count=count,
            sum_value=item.sum_value,
            process_id=process_id,
            scrape_seq=scrape_seq,
            scraped_at=scraped_at,
            table_id=item.table_id,
        )

    current_snapshot = _build(current)

    retro_series = [
        s for s in usable if s.retro and s.table_id != current.table_id
    ]
    previous: HistogramSnapshot | None = None
    if retro_series:
        try:
            previous = _build(retro_series[0])
        except MetricsParseError:
            previous = None

    if previous is not None:
        current_snapshot = HistogramSnapshot(
            metric_name=current_snapshot.metric_name,
            buckets=current_snapshot.buckets,
            count=current_snapshot.count,
            sum_value=current_snapshot.sum_value,
            process_id=current_snapshot.process_id,
            scrape_seq=current_snapshot.scrape_seq,
            scraped_at=current_snapshot.scraped_at,
            table_id=current_snapshot.table_id,
            previous=previous,
        )
    return current_snapshot


# --------------------------------------------------------------------------- #
# Bucket increments
# --------------------------------------------------------------------------- #

def _snapshot_totals(earlier: HistogramSnapshot, later: HistogramSnapshot) -> tuple[int, float]:
    delta_inf = later.count - earlier.count
    if delta_inf < 0:
        raise MetricsParseError("counter reset detected on _count")
    delta_sum = max(0.0, later.sum_value - earlier.sum_value)
    return delta_inf, delta_sum


def _segment_from(
    earlier: HistogramSnapshot,
    later: HistogramSnapshot,
    table_id: str | None,
    *,
    from_zero_baseline: bool = False,
) -> HistogramSegmentDelta:
    """Increment between two snapshots that share one bucket layout."""

    if list(earlier.buckets.keys()) != list(later.buckets.keys()):
        raise BucketTransitionError(
            "bucket boundaries differ inside one table; "
            "they must come from the same bucket table"
        )

    boundaries = [b for b in later.buckets if math.isfinite(b)]
    delta_buckets: list[int] = []
    for boundary in boundaries:
        diff = later.buckets[boundary] - earlier.buckets[boundary]
        if diff < 0:
            raise MetricsParseError(
                f"counter reset detected at le={boundary}; "
                "the two scrapes are not from the same process lifetime"
            )
        delta_buckets.append(diff)

    delta_inf, delta_sum = _snapshot_totals(earlier, later)
    return HistogramSegmentDelta(
        table_id=table_id,
        boundaries=boundaries,
        delta_buckets=delta_buckets,
        delta_inf=delta_inf,
        delta_sum=delta_sum,
        earlier=earlier,
        later=later,
        from_zero_baseline=from_zero_baseline,
    )


def _zero_baseline(later: HistogramSnapshot, table_id: str | None) -> HistogramSnapshot:
    """Synthetic all-zero snapshot for a table that just came into effect.

    A switched table starts counting from zero, so every count observed in
    the later scrape belongs to the window (all of it after the switch).
    """

    return HistogramSnapshot(
        metric_name=later.metric_name,
        buckets={boundary: 0 for boundary in later.buckets},
        count=0,
        sum_value=0.0,
        process_id=later.process_id,
        scrape_seq=None,
        scraped_at=later.scraped_at,
        table_id=table_id,
    )


def compute_delta(earlier: HistogramSnapshot, later: HistogramSnapshot) -> HistogramDelta:
    """Incremental counts between two cumulative snapshots of one process.

    Normally both scrapes share one bucket table.  If the target switched its
    table in between, the later scrape also exposes the frozen counters of
    the immediately preceding table (``later.previous``): the window is then
    split into an old-table segment (frozen old counts minus earlier counts)
    and a new-table segment (new counts, which start at zero at the switch).
    Every request is counted exactly once, under the table active when it was
    observed.

    Counter resets are guarded against defensively even though same-process
    selection already excludes restart windows.
    """

    if earlier.metric_name != later.metric_name:
        raise MetricsParseError("snapshots describe different metrics")

    if earlier.table_id == later.table_id:
        if list(earlier.buckets.keys()) != list(later.buckets.keys()):
            raise BucketTransitionError(
                "bucket boundaries differ between the two scrapes although "
                "they claim the same table id"
            )
        segment = _segment_from(earlier, later, later.table_id)
        return HistogramDelta(
            metric_name=later.metric_name,
            segments=[segment],
            earlier=earlier,
            later=later,
        )

    # Table changed between the two scrapes: it must be bridged by the one
    # retro table the target is allowed to retain.
    retro = later.previous
    if retro is None or retro.table_id != earlier.table_id:
        raise BucketTransitionError(
            "bucket table changed between the two scrapes and the frozen "
            "old-table counts needed to split the window are missing"
        )
    if list(earlier.buckets.keys()) != list(retro.buckets.keys()):
        raise BucketTransitionError(
            "frozen old-table counts do not match the earlier scrape's layout"
        )

    old_segment = _segment_from(earlier, retro, earlier.table_id)
    new_segment = _segment_from(
        _zero_baseline(later, later.table_id), later, later.table_id,
        from_zero_baseline=True,
    )
    return HistogramDelta(
        metric_name=later.metric_name,
        segments=[old_segment, new_segment],
        earlier=earlier,
        later=later,
    )


# --------------------------------------------------------------------------- #
# SLO bounds
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class Interval:
    """Closed-ish interval [lower, upper] of request counts/ratios."""

    lower: float
    upper: float

    def __post_init__(self) -> None:
        if self.lower < -1e-12 or self.upper < self.lower - 1e-12:
            raise ValueError(f"invalid interval: {self.lower}..{self.upper}")


def segment_bounds(segment: HistogramSegmentDelta, threshold_s: float) -> Interval:
    """Bounds on compliant requests inside one bucket-table segment.

    Prometheus only tells us which *bucket* each request fell into, so when
    the threshold cuts through a bucket the exact answer is unknown and an
    uncertainty range is returned.

    Given cumulative bucket counts ``C(b)`` (# requests <= b):

    * requests definitely <= threshold:  C(largest boundary <= threshold)
    * requests possibly   <= threshold:  C(smallest boundary >= threshold)
      (a threshold equal to a boundary makes that bucket fully known, so the
      two coincide)
    """

    if threshold_s < 0:
        raise ValueError("threshold must be non-negative")

    # delta_buckets are per-upper-bound *cumulative* counts of the segment;
    # the +Inf equivalent is delta_inf.
    definitely = 0
    possibly: int | None = None

    for boundary, cumulative in zip(segment.boundaries, segment.delta_buckets):
        if boundary < threshold_s:
            definitely = cumulative
        elif boundary == threshold_s:
            # Threshold on a boundary: everything up to it is known.
            definitely = cumulative
            possibly = cumulative
        else:  # boundary > threshold_s: first such bucket straddles it
            if possibly is None:
                possibly = cumulative

    if possibly is None:
        # Threshold above the largest finite boundary: the tail bucket is
        # (largest_bound, +Inf), every request there may or may not qualify.
        possibly = segment.delta_inf

    return Interval(lower=definitely, upper=possibly)


def compliant_count_bounds(delta: HistogramDelta, threshold_s: float) -> Interval:
    """Bounds on compliant requests across all segments of one instance.

    Each segment is scored against the bucket table that was active for it;
    the per-segment bounds are then added together (independent uncertainty
    adds up).
    """

    lower = 0.0
    upper = 0.0
    for segment in delta.segments:
        bounds = segment_bounds(segment, threshold_s)
        lower += bounds.lower
        upper += bounds.upper
    return Interval(lower=lower, upper=upper)


@dataclass
class CombinedResult:
    total_requests: int
    compliant_lower: int
    compliant_upper: int
    lower_ratio: float
    upper_ratio: float
    per_instance: dict[str, Interval] = field(default_factory=dict)

    @property
    def is_zero_traffic(self) -> bool:
        return self.total_requests == 0


def combine_instances(
    deltas: dict[str, HistogramDelta], threshold_s: float
) -> CombinedResult:
    """Sum SLO bounds across instances with potentially different buckets."""

    per_instance: dict[str, Interval] = {}
    total = 0
    lower = 0
    upper = 0
    for instance, delta in deltas.items():
        bounds = compliant_count_bounds(delta, threshold_s)
        per_instance[instance] = bounds
        total += delta.total_requests
        lower += int(bounds.lower)
        upper += int(bounds.upper)

    if total == 0:
        lower_ratio = upper_ratio = 0.0
    else:
        lower_ratio = lower / total
        upper_ratio = upper / total

    return CombinedResult(
        total_requests=total,
        compliant_lower=lower,
        compliant_upper=upper,
        lower_ratio=lower_ratio,
        upper_ratio=upper_ratio,
        per_instance=per_instance,
    )
