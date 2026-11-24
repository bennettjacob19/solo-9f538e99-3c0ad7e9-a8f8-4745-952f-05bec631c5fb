"""Shared scrape store and the verification orchestration.

A single background task scrapes every configured instance on a fixed
cadence (>= 1s apart per instance).  All concurrent ``/check`` requests read
from the same in-memory store, so checks share samples and one request
timing out never disturbs another.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import httpx

from .buckets import validate_boundaries
from .histogram import (
    BucketTransitionError,
    HistogramDelta,
    HistogramSnapshot,
    MetricsParseError,
    compute_delta,
    parse_metrics_text,
)

logger = logging.getLogger(__name__)

METRIC_NAME = "http_request_duration_seconds"
# Same-instance scrapes must be at least this far apart (seconds).
MIN_SAMPLE_GAP = 1.0
# A scrape is only usable for this long after it happened.
FRESHNESS_WINDOW = 3.0
# How often the background loop scrapes each instance.
SCRAPE_INTERVAL = 1.05
# Keep the last N scrapes per instance (only the newest two are used).
HISTORY_LIMIT = 8
HTTP_TIMEOUT = 2.0
# Per-instance control endpoint path (relative to the scrape URL's base).
BUCKETS_PATH = "/buckets"


@dataclass
class ScrapeRecord:
    ok: bool
    at: float
    snapshot: HistogramSnapshot | None = None
    error: str | None = None


@dataclass
class InstanceState:
    name: str
    url: str
    history: list[ScrapeRecord] = field(default_factory=list)
    total_attempts: int = 0
    total_successes: int = 0
    total_failures: int = 0
    # Last bucket table this instance was verified to be serving.  Once set,
    # a further switch submission waits until a scrape actually shows this
    # table ("核查器收到新桶表前拒绝再次换桶").
    confirmed_table_id: str | None = None
    # Table id most recently submitted to this instance, for GET /buckets.
    # ``"pending"`` until a scrape actually shows a different table, then the
    # received table id.
    submitted_table: str | None = None
    submitted_from_table: str | None = None
    submitted_boundaries: list[float] | None = None
    submitted_at: float | None = None
    submitted_process_id: str | None = None

    @property
    def latest(self) -> ScrapeRecord | None:
        return self.history[-1] if self.history else None

    @property
    def control_base(self) -> str:
        """Base URL of the sample target (strip the ``/metrics`` suffix)."""
        if self.url.endswith("/metrics"):
            return self.url[: -len("/metrics")]
        return self.url.rstrip("/")


class SwitchRejected(RuntimeError):
    """A bucket-table switch was refused; the current table stays in force."""


def _safe_json(response: httpx.Response) -> dict | None:
    try:
        value = response.json()
    except ValueError:
        return None
    return value if isinstance(value, dict) else None


def _matches_boundaries(snapshot: HistogramSnapshot, boundaries: list[float] | None) -> bool:
    """True when a scrape carries exactly the submitted boundary list."""

    if boundaries is None:
        return False
    observed = [b for b in snapshot.buckets if math.isfinite(b)]
    return len(observed) == len(boundaries) and all(
        abs(a - b) <= 1e-12 for a, b in zip(observed, boundaries)
    )


@dataclass
class ReadySample:
    """The two validated scrapes for one instance."""

    earlier: ScrapeRecord
    later: ScrapeRecord
    delta: HistogramDelta

    @property
    def sample_times(self) -> tuple[float, float]:
        return self.earlier.at, self.later.at

    @property
    def crosses_table_change(self) -> bool:
        return self.delta.crosses_table_change

    @property
    def table_ids(self) -> list[str | None]:
        return self.delta.table_ids


class ScrapeStore:
    def __init__(
        self,
        instances: dict[str, str],
        metric_name: str = METRIC_NAME,
        scrape_interval: float = SCRAPE_INTERVAL,
        min_gap: float = MIN_SAMPLE_GAP,
        freshness: float = FRESHNESS_WINDOW,
    ) -> None:
        self.instances = {
            name: InstanceState(name=name, url=url) for name, url in instances.items()
        }
        self.metric_name = metric_name
        self.scrape_interval = scrape_interval
        self.min_gap = min_gap
        self.freshness = freshness
        self._cond = asyncio.Condition()
        self._task: asyncio.Task | None = None
        self._client: httpx.AsyncClient | None = None
        # Serialises the check-then-submit sequence for bucket switches, so
        # two concurrent submissions cannot both pass the pending gate.
        self._switch_lock = asyncio.Lock()

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        if self._task is None:
            self._client = httpx.AsyncClient(timeout=HTTP_TIMEOUT)
            self._task = asyncio.create_task(self._scrape_loop(), name="scrape-loop")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _scrape_once(self, state: InstanceState) -> None:
        assert self._client is not None
        state.total_attempts += 1
        try:
            response = await self._client.get(state.url)
            response.raise_for_status()
            snapshot = parse_metrics_text(
                response.text, self.metric_name, time.time()
            )
            record = ScrapeRecord(ok=True, at=snapshot.scraped_at, snapshot=snapshot)
            state.total_successes += 1
            if snapshot.table_id is not None:
                state.confirmed_table_id = snapshot.table_id
        except (httpx.HTTPError, MetricsParseError, ValueError) as exc:
            record = ScrapeRecord(ok=False, at=time.time(), error=f"{type(exc).__name__}: {exc}")
            state.total_failures += 1
        except Exception as exc:  # defensive: one bad scrape must not kill the loop
            logger.exception("unexpected scrape failure for %s", state.name)
            record = ScrapeRecord(ok=False, at=time.time(), error=f"{type(exc).__name__}: {exc}")
            state.total_failures += 1

        state.history.append(record)
        if len(state.history) > HISTORY_LIMIT:
            del state.history[: len(state.history) - HISTORY_LIMIT]

    async def _scrape_loop(self) -> None:
        while True:
            await asyncio.gather(
                *(self._scrape_once(state) for state in self.instances.values()),
                return_exceptions=True,
            )
            async with self._cond:
                self._cond.notify_all()
            await asyncio.sleep(self.scrape_interval)

    # ------------------------------------------------------------------ #
    # Read side
    # ------------------------------------------------------------------ #

    def sample_status(self, name: str, now: float | None = None) -> ReadySample | str:
        """Public wrapper around :meth:`_select_sample` for observability."""
        return self._select_sample(
            self.instances[name], time.time() if now is None else now
        )

    def _select_sample(self, state: InstanceState, now: float) -> ReadySample | str:
        """Validate the two most recent scrapes for one instance.

        Returns a :class:`ReadySample`, or a machine-readable reason string.
        """

        records = [r for r in state.history if r.ok]
        if len(records) < 2:
            return "insufficient_scrapes"
        earlier, later = records[-2], records[-1]
        assert earlier.snapshot is not None and later.snapshot is not None

        if now - later.at > self.freshness:
            return "stale_scrape"
        if now - earlier.at > self.freshness:
            return "stale_scrape"
        if later.at - earlier.at < self.min_gap:
            return "samples_too_close"
        if (
            earlier.snapshot.process_id is None
            or later.snapshot.process_id is None
            or earlier.snapshot.process_id != later.snapshot.process_id
        ):
            return "process_restarted"
        try:
            delta = compute_delta(earlier.snapshot, later.snapshot)
        except BucketTransitionError:
            # A bucket switch happened without a usable old-table bridge.
            return "bucket_table_changed"
        except MetricsParseError:
            # A counter reset means a restart slipped past process labels.
            return "process_restarted"
        return ReadySample(earlier=earlier, later=later, delta=delta)

    async def wait_for_samples(
        self, deadline: float
    ) -> tuple[dict[str, ReadySample], list[str]]:
        """Wait until every instance has a valid two-scrape sample or timeout.

        Concurrent calls block on the same condition variable and therefore
        share every scrape the background task produces.
        """

        while True:
            ready: dict[str, ReadySample] = {}
            missing: list[str] = []
            wall_now = time.time()
            for name, state in self.instances.items():
                outcome = self._select_sample(state, wall_now)
                if isinstance(outcome, ReadySample):
                    ready[name] = outcome
                else:
                    missing.append(name)

            if not missing:
                return ready, []
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return ready, missing

            async with self._cond:
                try:
                    await asyncio.wait_for(
                        self._cond.wait(),
                        timeout=max(0.0, deadline - time.monotonic()),
                    )
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass

    # ------------------------------------------------------------------ #
    # Bucket-table switches
    # ------------------------------------------------------------------ #

    def _publish_gate(self, name: str) -> str | None:
        """Return a rejection reason if a switch must not be submitted yet.

        The verifier only accepts a further table once it has *received* the
        previously submitted one, i.e. a scrape shows the submitted source
        table ("核查器收到新桶表前拒绝再次换桶").  Receipt is matched by the
        boundary list, so an unrelated table from a restarted target can never
        satisfy the gate.
        """

        state = self.instances[name]
        if state.submitted_table != "pending":
            return None
        latest = state.latest
        if (
            latest is not None
            and latest.ok
            and latest.snapshot is not None
            and _matches_boundaries(latest.snapshot, state.submitted_boundaries)
            and (
                latest.snapshot.table_id is None
                or latest.snapshot.table_id != state.submitted_from_table
            )
        ):
            # The submitted table has been observed: the switch is complete.
            state.submitted_table = latest.snapshot.table_id or "received"
            state.confirmed_table_id = latest.snapshot.table_id
            return None
        if (
            latest is not None
            and latest.ok
            and latest.snapshot is not None
            and state.submitted_process_id is not None
            and latest.snapshot.process_id != state.submitted_process_id
        ):
            # The sample restarted after the submission but never reported the
            # submitted table: it is gone.  Reopen the gate instead of wedging
            # it forever; the lost switch is surfaced through /buckets.
            state.submitted_table = None
            return None
        return (
            "上一份新桶表尚未在抓取中出现（核查器还没收到新桶表），"
            "拒绝再次换桶"
        )

    def switch_block_reason(self, name: str) -> str | None:
        """Public gate check: ``None`` means a further switch is allowed."""
        return self._publish_gate(name)

    async def submit_bucket_table(
        self, name: str, boundaries: list[float]
    ) -> dict:
        """Validate and forward a new bucket table to one sample target.

        The switch is requested through the target's ``POST /buckets``.  It
        takes effect at the target's next successful scrape, at the latest.
        Raises :class:`BucketValidationError` for an invalid table and
        :class:`SwitchRejected` if the previous switch has not been observed
        or another submission is already in flight.
        """

        validated = validate_boundaries(boundaries)
        if name not in self.instances:
            raise KeyError(name)
        if self._client is None:
            raise RuntimeError("store is not running")

        async with self._switch_lock:
            return await self._submit_locked(name, validated)

    async def _submit_locked(self, name: str, validated: list[float]) -> dict:
        reason = self._publish_gate(name)
        if reason is not None:
            raise SwitchRejected(reason)

        state = self.instances[name]
        url = f"{state.control_base}{BUCKETS_PATH}"
        previous_table_id = (
            state.latest.snapshot.table_id
            if state.latest is not None
            and state.latest.ok
            and state.latest.snapshot is not None
            else None
        )
        try:
            response = await self._client.post(
                url, json={"boundaries": validated}
            )
        except httpx.HTTPError as exc:
            raise SwitchRejected(f"无法提交桶表到 {name}: {exc}") from exc
        if response.status_code == 409:
            raise SwitchRejected(
                f"{name} 仍在等待上一次换桶生效，拒绝再次换桶"
            )
        if response.status_code == 400:
            detail = ""
            try:
                detail = str(response.json().get("detail", ""))
            except ValueError:
                detail = response.text
            raise BucketValidationError(detail or "桶表被样例目标拒绝")
        if response.status_code >= 400:
            raise SwitchRejected(
                f"提交桶表到 {name} 失败: HTTP {response.status_code} "
                f"{response.text[:200]}"
            )

        state.submitted_table = "pending"
        state.submitted_from_table = previous_table_id
        state.submitted_boundaries = validated
        state.submitted_at = time.time()
        state.submitted_process_id = (
            state.latest.snapshot.process_id
            if state.latest is not None
            and state.latest.ok
            and state.latest.snapshot is not None
            else None
        )
        async with self._cond:
            self._cond.notify_all()
        return {
            "instance": name,
            "accepted": True,
            "boundaries": validated,
            "previous_table_id": previous_table_id,
            "target_response": _safe_json(response),
        }

    async def submit_bucket_table_all(
        self, names: list[str], boundaries: list[float]
    ) -> list[dict]:
        """Submit the same table to several instances, all-or-nothing.

        Every instance is validated and gate-checked before the first request
        is sent, so an invalid table or a not-yet-received previous switch
        cannot leave the group half-switched.
        """

        validated = validate_boundaries(boundaries)
        unknown = [name for name in names if name not in self.instances]
        if unknown:
            raise KeyError(unknown[0])
        if self._client is None:
            raise RuntimeError("store is not running")

        async with self._switch_lock:
            for name in names:
                reason = self._publish_gate(name)
                if reason is not None:
                    raise SwitchRejected(f"{name}: {reason}")
            # Pre-flight passed for the whole group; now submit each one.
            # A target that accepted the table cannot be rolled back, so a
            # mid-flight failure is reported rather than silently retried.
            return [
                await self._submit_locked(name, validated) for name in names
            ]

    async def wait_until_table_changed(
        self, name: str, previous_table_id: str | None, deadline: float
    ) -> HistogramSnapshot | None:
        """Wait for a scrape whose bucket table differs from ``previous``.

        Returns the first such snapshot, or ``None`` on timeout.  This is how
        the verifier learns that a submitted table has actually been received
        ("收到新桶表"), after which another switch may be submitted.
        """

        while True:
            state = self.instances[name]
            latest = state.latest
            if (
                latest is not None
                and latest.ok
                and latest.snapshot is not None
                and latest.snapshot.table_id is not None
                and latest.snapshot.table_id != previous_table_id
            ):
                state.confirmed_table_id = latest.snapshot.table_id
                state.submitted_table = latest.snapshot.table_id
                return latest.snapshot
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            async with self._cond:
                try:
                    await asyncio.wait_for(self._cond.wait(), timeout=remaining)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass

    async def fetch_target_switch_status(self, name: str) -> dict | None:
        """Ask the sample target itself for its switch progress.

        Returns ``None`` when the target cannot be reached or does not expose
        the control endpoint (e.g. an external Prometheus target).
        """

        state = self.instances[name]
        if self._client is None:
            return None
        try:
            response = await self._client.get(
                f"{state.control_base}{BUCKETS_PATH}"
            )
            if response.status_code >= 400:
                return None
            body = response.json()
        except (httpx.HTTPError, ValueError):
            return None
        if not isinstance(body, dict):
            return None
        return {
            "status": body.get("status"),
            "current_table": body.get("current_table"),
            "retro_table": body.get("retro_table"),
            "pending_boundaries": body.get("pending_boundaries"),
            "last_switch_at": body.get("last_switch_at"),
            "switch_count": body.get("switch_count"),
        }

    # ------------------------------------------------------------------ #
    # Observability
    # ------------------------------------------------------------------ #

    def stats(self) -> dict:
        result: dict = {"instances": {}}
        for name, state in self.instances.items():
            last_ok = next(
                (r for r in reversed(state.history) if r.ok), None
            )
            result["instances"][name] = {
                "url": state.url,
                "scrape_attempts": state.total_attempts,
                "scrape_successes": state.total_successes,
                "scrape_failures": state.total_failures,
                "history_len": len(state.history),
                "latest_scrape_at": state.latest.at if state.latest else None,
                "latest_ok": state.latest.ok if state.latest else None,
                "latest_error": state.latest.error if state.latest else None,
                "last_success_at": last_ok.at if last_ok else None,
                "last_process_id": (
                    last_ok.snapshot.process_id if last_ok and last_ok.snapshot else None
                ),
                "current_table_id": state.confirmed_table_id,
                "submitted_table": state.submitted_table,
                "submitted_from_table": state.submitted_from_table,
                "submitted_boundaries": state.submitted_boundaries,
                "submitted_at": state.submitted_at,
            }
        return result


@asynccontextmanager
async def store_lifespan(store: ScrapeStore):
    await store.start()
    try:
        yield
    finally:
        await store.stop()
