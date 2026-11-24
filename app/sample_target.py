"""Local sample target exposing a Prometheus latency histogram.

Two instances of this app are started with different ``SAMPLE_BUCKETS``
values so the verifier has to cope with differing bucket layouts.

Runtime controls (used by tests and demos):

* ``POST /inject``  -- add one or more request observations
                       body: ``{"duration_seconds": 0.23, "count": 1}``
* ``POST /buckets`` -- submit a new bucket table (strictly increasing finite
                       positive boundaries).  It takes effect at the next
                       successful scrapes.  An invalid submission is rejected
                       and the current table is kept unchanged.
* ``GET  /buckets`` -- switch progress: pending / applied table, table ids
* ``POST /restart`` -- simulate a process restart: new run id, new process
                       start time, all counters reset to zero
* ``GET  /state``   -- inspect the in-memory counters
"""

from __future__ import annotations

import os
import threading
import time
import uuid
from dataclasses import dataclass, field

from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from .buckets import BucketValidationError, validate_boundaries

METRIC_NAME = "http_request_duration_seconds"
DEFAULT_BUCKETS_A = "0.05,0.1,0.25,0.5,1,2.5,5"
DEFAULT_BUCKETS_B = "0.1,0.3,0.7,1.5,3,6"


class InjectBody(BaseModel):
    duration_seconds: float = Field(..., ge=0)
    count: int = Field(1, ge=1, le=100_000)


class BucketTableBody(BaseModel):
    boundaries: list[float] = Field(
        ...,
        description=(
            "严格递增的有限正数桶边界（秒），例如 [0.05, 0.1, 0.5, 1, 5]"
        ),
    )


@dataclass
class BucketTableCounts:
    """Cumulative counters belonging to one bucket table."""

    table_id: str
    boundaries: list[float]
    bucket_counts: dict[float, int]
    total_count: int = 0
    total_sum: float = 0.0

    @classmethod
    def fresh(cls, table_id: str, boundaries: list[float]) -> "BucketTableCounts":
        return cls(
            table_id=table_id,
            boundaries=list(boundaries),
            bucket_counts={b: 0 for b in boundaries},
        )

    def inject(self, duration: float, count: int) -> None:
        for boundary in self.boundaries:
            if duration <= boundary:
                self.bucket_counts[boundary] += count
        self.total_count += count
        self.total_sum += duration * count

    def freeze(self) -> "BucketTableCounts":
        return BucketTableCounts(
            table_id=self.table_id,
            boundaries=list(self.boundaries),
            bucket_counts=dict(self.bucket_counts),
            total_count=self.total_count,
            total_sum=self.total_sum,
        )


@dataclass
class TargetState:
    """Mutable sample-target state, guarded by a lock.

    One switch may be pending at a time.  A switch takes effect exactly when
    the next ``/metrics`` response is rendered: the old table's cumulative
    counts are frozen into ``retro`` (at most one is kept) and the new table
    starts from zero.  Injections are always applied to whichever table is
    currently active, so every request is counted exactly once.
    """

    instance_id: str
    initial_boundaries: list[float]
    # Distinguishes this target *process* from a later one, so a real
    # container restart can never reuse a previous table id.  It deliberately
    # survives the simulated ``/restart`` (which only resets counters).
    target_uid: str = field(init=False)
    current: BucketTableCounts = field(init=False)
    retro: BucketTableCounts | None = field(default=None, init=False)
    pending_boundaries: list[float] | None = field(default=None, init=False)
    pending_submitted_at: float | None = field(default=None, init=False)
    last_switch_at: float | None = field(default=None, init=False)
    switch_count: int = field(default=0, init=False)
    table_serial: int = field(default=1, init=False)
    lock: threading.RLock = field(default_factory=threading.RLock, init=False)
    run_id: str = field(init=False)
    process_start: float = field(init=False)
    scrape_seq: int = field(default=0, init=False)
    restart_count: int = field(default=0, init=False)

    def _next_table_id(self) -> str:
        return f"{self.target_uid}-t{self.table_serial}"

    def __post_init__(self) -> None:
        self.target_uid = uuid.uuid4().hex[:6]
        self.current = BucketTableCounts.fresh(
            self._next_table_id(), self.initial_boundaries
        )
        self.reset()

    # -- process lifecycle -------------------------------------------------- #

    def reset(self) -> None:
        """Restart the (simulated) process.

        The bucket layout survives a restart (configuration, not state), but
        all counters start at zero and any frozen old-table counts belong to
        the previous process, so they are discarded.  A pending switch still
        takes effect at the next scrape.
        """
        with self.lock:
            self.run_id = uuid.uuid4().hex[:12]
            self.process_start = time.time()
            self.current = BucketTableCounts.fresh(
                self.current.table_id, self.current.boundaries
            )
            self.retro = None
            self.restart_count += 1

    # -- traffic ------------------------------------------------------------ #

    def inject(self, duration: float, count: int) -> None:
        with self.lock:
            self.current.inject(duration, count)

    # -- bucket table switches --------------------------------------------- #

    def request_switch(self, boundaries: list[float]) -> dict:
        """Queue a validated table; reject a second one before it is served."""
        validated = validate_boundaries(boundaries)  # may raise
        with self.lock:
            if self.pending_boundaries is not None:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "已有一份桶表等待在下一次成功抓取时生效；"
                        "核查器收到新桶表前拒绝再次换桶。"
                    ),
                )
            self.pending_boundaries = validated
            self.pending_submitted_at = time.time()
            return self.switch_status()

    def _apply_pending_switch(self) -> None:
        """Apply the pending table right before rendering a scrape."""
        with self.lock:
            if self.pending_boundaries is None:
                return
            new_boundaries = self.pending_boundaries
            self.pending_boundaries = None
            self.pending_submitted_at = None
            self.last_switch_at = time.time()
            self.switch_count += 1
            # Freeze the outgoing table's counters; at most one retro table is
            # retained, which is exactly the old-table bridge we hand out.
            # Even a submission with identical boundaries starts a new table
            # (new id, counters from zero) so that the switch is observable.
            self.retro = self.current.freeze()
            self.table_serial += 1
            self.current = BucketTableCounts.fresh(
                self._next_table_id(), new_boundaries
            )

    def render_metrics(self) -> str:
        """Render one scrape; a pending switch becomes effective here."""
        with self.lock:
            self._apply_pending_switch()
            state = self.current
            retro = self.retro
            run_id = self.run_id
            lines = [
                f"# HELP {METRIC_NAME} request latency in seconds",
                f"# TYPE {METRIC_NAME} histogram",
            ]

            def _series(table: BucketTableCounts, retro_label: bool) -> None:
                extra = ',retro="true"' if retro_label else ""
                for boundary, value in table.bucket_counts.items():
                    lines.append(
                        f'{METRIC_NAME}_bucket{{le="{boundary:g}",'
                        f'run_id="{run_id}",bucket_table="{table.table_id}"'
                        f"{extra}}} {value}"
                    )
                lines.append(
                    f'{METRIC_NAME}_bucket{{le="+Inf",run_id="{run_id}",'
                    f'bucket_table="{table.table_id}"{extra}}} {table.total_count}'
                )
                lines.append(
                    f'{METRIC_NAME}_count{{run_id="{run_id}",'
                    f'bucket_table="{table.table_id}"{extra}}} {table.total_count}'
                )
                lines.append(
                    f'{METRIC_NAME}_sum{{run_id="{run_id}",'
                    f'bucket_table="{table.table_id}"{extra}}} {table.total_sum}'
                )

            _series(state, retro_label=False)
            if retro is not None:
                _series(retro, retro_label=True)

            lines.extend(
                [
                    "# HELP process_start_time_seconds start time of the process since unix epoch",
                    "# TYPE process_start_time_seconds gauge",
                    f"process_start_time_seconds {self.process_start:.6f}",
                    "# HELP sample_scrape_seq number of scrapes served since process start",
                    "# TYPE sample_scrape_seq gauge",
                    f"sample_scrape_seq {self.scrape_seq}",
                ]
            )
            self.scrape_seq += 1
            return "\n".join(lines) + "\n"

    # -- observability ------------------------------------------------------ #

    def switch_status(self) -> dict:
        with self.lock:
            return {
                "instance_id": self.instance_id,
                "status": (
                    "pending" if self.pending_boundaries is not None else "idle"
                ),
                "current_table": {
                    "table_id": self.current.table_id,
                    "boundaries": list(self.current.boundaries),
                    "requests": self.current.total_count,
                },
                "retro_table": (
                    None
                    if self.retro is None
                    else {
                        "table_id": self.retro.table_id,
                        "boundaries": list(self.retro.boundaries),
                        "requests": self.retro.total_count,
                    }
                ),
                "pending_boundaries": (
                    None
                    if self.pending_boundaries is None
                    else list(self.pending_boundaries)
                ),
                "pending_submitted_at": self.pending_submitted_at,
                "last_switch_at": self.last_switch_at,
                "switch_count": self.switch_count,
                "note": (
                    "待生效的桶表会在下一次成功抓取（GET /metrics）时生效；"
                    "生效前再次提交会被拒绝。"
                ),
            }

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "instance_id": self.instance_id,
                "run_id": self.run_id,
                "process_start_time_seconds": self.process_start,
                "table_id": self.current.table_id,
                "boundaries": list(self.current.boundaries),
                "bucket_counts": dict(self.current.bucket_counts),
                "count": self.current.total_count,
                "sum": self.current.total_sum,
                "retro_table_id": self.retro.table_id if self.retro else None,
                "retro_bucket_counts": (
                    dict(self.retro.bucket_counts) if self.retro else None
                ),
                "retro_count": self.retro.total_count if self.retro else None,
                "pending_boundaries": (
                    None
                    if self.pending_boundaries is None
                    else list(self.pending_boundaries)
                ),
                "switch_count": self.switch_count,
                "restart_count": self.restart_count,
                "scrape_seq": self.scrape_seq,
            }

    def switch_progress(self) -> dict:
        """Alias kept for clarity at the API layer."""
        return self.switch_status()


def _read_buckets(instance_id: str) -> list[float]:
    raw = os.environ.get("SAMPLE_BUCKETS")
    if raw is None:
        raw = DEFAULT_BUCKETS_A if instance_id.endswith("a") else DEFAULT_BUCKETS_B
    values = [float(part.strip()) for part in raw.split(",") if part.strip()]
    return validate_boundaries(values)


def create_app() -> FastAPI:
    instance_id = os.environ.get("INSTANCE_ID", "sample-a")
    state = TargetState(instance_id, _read_buckets(instance_id))
    app = FastAPI(title=f"sample-target-{instance_id}", version="1.1.0")
    app.state.target = state

    @app.post("/inject")
    def inject(body: InjectBody) -> dict:
        state.inject(body.duration_seconds, body.count)
        return {
            "ok": True,
            "table_id": state.current.table_id,
            "total_count": state.current.total_count,
        }

    @app.post("/restart")
    def restart() -> dict:
        state.reset()
        return {"ok": True, "run_id": state.run_id, "table_id": state.current.table_id}

    @app.post("/buckets", tags=["换桶"])
    def submit_buckets(body: BucketTableBody) -> dict:
        """提交新桶表；下一次成功抓取时生效，非法提交保留原表。"""
        try:
            return state.request_switch(body.boundaries)
        except BucketValidationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/buckets", tags=["换桶"])
    def get_buckets() -> dict:
        """查看换桶进度：待生效 / 当前 / 旧表。"""
        return state.switch_progress()

    @app.get("/state")
    def get_state() -> dict:
        return state.snapshot()

    # Plain-text exposition, returned with the Prometheus content type.
    @app.get("/metrics", response_class=PlainTextResponse)
    def render_metrics() -> PlainTextResponse:
        body = state.render_metrics()
        return PlainTextResponse(body, media_type="text/plain; version=0.0.4")

    return app


app = create_app()
