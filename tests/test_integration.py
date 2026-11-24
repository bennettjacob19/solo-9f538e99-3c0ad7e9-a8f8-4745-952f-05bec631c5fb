"""End-to-end tests with real uvicorn servers and background scraping."""

from __future__ import annotations

import asyncio
import os
import time

import httpx
import pytest
import uvicorn

from app.main import create_app
from app.sample_target import create_app as create_target_app

PORTS = {"a": 9201, "b": 9202}
BUCKETS_A = "0.05,0.1,0.25,0.5,1,2.5,5"
BUCKETS_B = "0.1,0.3,0.7,1.5,3,6"


class Server:
    def __init__(self, name: str, port: int, app_factory, env: dict[str, str] | None = None):
        self.name = name
        self.port = port
        # Factory is invoked inside start() *after* the env vars are set,
        # otherwise SAMPLE_BUCKETS / INSTANCE_ID would not take effect.
        self.app_factory = app_factory
        self.env = env or {}

    async def start(self) -> None:
        old = {k: os.environ.get(k) for k in self.env}
        os.environ.update(self.env)
        try:
            app = self.app_factory()
        finally:
            for k, v in old.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
        self.config = uvicorn.Config(
            app, host="127.0.0.1", port=self.port, log_level="error", lifespan="on"
        )
        self.server = uvicorn.Server(self.config)
        self._task = asyncio.create_task(self.server.serve())
        await _wait_port(self.port)

    async def stop(self) -> None:
        self.server.should_exit = True
        await asyncio.wait_for(self._task, timeout=5)


async def _wait_port(port: int, timeout: float = 8.0) -> None:
    deadline = time.monotonic() + timeout
    last_exc = None
    async with httpx.AsyncClient() as client:
        while time.monotonic() < deadline:
            try:
                await client.get(f"http://127.0.0.1:{port}/state")
                return
            except httpx.HTTPError as exc:
                last_exc = exc
                await asyncio.sleep(0.05)
    raise RuntimeError(f"port {port} never came up: {last_exc}")


@pytest.fixture
async def servers():
    target_a = Server("a", PORTS["a"], create_target_app,
                      {"INSTANCE_ID": "sample-a", "SAMPLE_BUCKETS": BUCKETS_A})
    target_b = Server("b", PORTS["b"], create_target_app,
                      {"INSTANCE_ID": "sample-b", "SAMPLE_BUCKETS": BUCKETS_B})
    await asyncio.gather(target_a.start(), target_b.start())

    os.environ["SCRAPE_TARGETS"] = (
        f"sample-a=http://127.0.0.1:{PORTS['a']}/metrics,"
        f"sample-b=http://127.0.0.1:{PORTS['b']}/metrics"
    )
    verifier = Server("verifier", 9200, create_app)
    await verifier.start()

    yield {
        "verifier": "http://127.0.0.1:9200",
        "a": f"http://127.0.0.1:{PORTS['a']}",
        "b": f"http://127.0.0.1:{PORTS['b']}",
    }

    await verifier.stop()
    await asyncio.gather(target_a.stop(), target_b.stop())
    os.environ.pop("SCRAPE_TARGETS", None)


async def _inject(base: str, duration: float, count: int = 1) -> None:
    async with httpx.AsyncClient() as client:
        await client.post(
            f"{base}/inject",
            json={"duration_seconds": duration, "count": count},
        )


async def _check(base: str, threshold: float, max_wait: float, target: float = 0.99):
    async with httpx.AsyncClient(timeout=max_wait + 10) as client:
        resp = await client.get(
            f"{base}/check",
            params={"threshold": threshold, "max-wait": max_wait, "target": target},
        )
        return resp.json()


async def _wait_ready(base: str, timeout: float = 8.0) -> None:
    deadline = time.monotonic() + timeout
    async with httpx.AsyncClient() as client:
        while time.monotonic() < deadline:
            stats = (await client.get(f"{base}/stats")).json()
            samples = stats["current_ready_samples"]
            if all("not_ready_reason" not in v for v in samples.values()):
                return
            await asyncio.sleep(0.1)
    raise AssertionError("verifier never got ready samples")


async def _wait_windows(base: str, expected: dict[str, int], timeout: float = 10.0):
    """Wait until each instance's current window holds the expected count."""

    deadline = time.monotonic() + timeout
    async with httpx.AsyncClient() as client:
        while time.monotonic() < deadline:
            stats = (await client.get(f"{base}/stats")).json()
            samples = stats["current_ready_samples"]
            if all(
                isinstance(value, dict)
                and value.get("requests_in_window") == expected[name]
                for name, value in samples.items()
            ):
                return stats
            await asyncio.sleep(0.1)
    raise AssertionError(f"scrape windows never reached {expected}: {samples}")


async def _wait_fresh_scrape(base: str, names, timeout: float = 5.0):
    """Block until a background scrape completes *now*.

    Act immediately afterwards and the next scrape (~1.05s later) is safely
    in the future, which makes "one window straddles the switch" deterministic
    instead of a race against the scrape cadence.
    """

    started = time.time()
    deadline = time.monotonic() + timeout
    async with httpx.AsyncClient() as client:
        while time.monotonic() < deadline:
            stats = (await client.get(f"{base}/stats")).json()
            times = [
                stats["instances"][name].get("latest_scrape_at") for name in names
            ]
            if all(t is not None and t >= started for t in times):
                return
            await asyncio.sleep(0.01)
    raise AssertionError("no fresh scrape observed")


# --------------------------------------------------------------------------- #


async def test_proceed_when_lower_bound_meets_99_percent(servers):
    # 100 reqs per instance, exactly 99 within 0.5s on each -> 198/200.
    await _inject(servers["a"], 0.2, 99)
    await _inject(servers["a"], 2.0, 1)
    await _inject(servers["b"], 0.2, 99)
    await _inject(servers["b"], 2.0, 1)

    result = await _check(servers["verifier"], threshold=0.5, max_wait=8)
    assert result["decision"] == "proceed", result
    assert result["total_requests"] == 200
    assert result["compliant_lower"] == 198
    assert result["lower_ratio"] == pytest.approx(0.99)
    assert result["timed_out"] is False
    # two scrapes, at least 1s apart, same process per instance
    for name in ("sample-a", "sample-b"):
        sample = result["instances"][name]
        assert sample["gap_seconds"] >= 1.0
        assert sample["process_id"]
        assert sample["earlier_scrape_at"] < sample["later_scrape_at"]


async def test_halt_when_upper_bound_below_99_percent(servers):
    # 90/100 good on each instance -> 180/200 = 90%, upper bound stays 90%.
    await _inject(servers["a"], 0.2, 90)
    await _inject(servers["a"], 2.0, 10)
    await _inject(servers["b"], 0.2, 90)
    await _inject(servers["b"], 2.0, 10)

    result = await _check(servers["verifier"], threshold=0.5, max_wait=8)
    assert result["decision"] == "halt", result
    assert result["upper_ratio"] == pytest.approx(0.9)
    assert result["lower_ratio"] == pytest.approx(0.9)


async def test_inconclusive_when_threshold_inside_bucket(servers):
    # The verifier scores the *increment* between the two most recent
    # scrapes. Restart both targets first so no traffic from earlier tests
    # leaks into the window, then wait for fresh same-process samples.
    async with httpx.AsyncClient() as client:
        await client.post(f"{servers['a']}/restart")
        await client.post(f"{servers['b']}/restart")
    await _wait_ready(servers["verifier"])

    # Instance A buckets include 0.5 -> exact at threshold 0.5:
    # 99 good, 1 slow = 99/100.
    await _inject(servers["a"], 0.2, 99)
    await _inject(servers["a"], 2.0, 1)
    # Instance B's nearest boundaries around 0.5 are 0.3 and 0.7.
    # 97 clearly <= 0.3; three requests in (0.3, 0.7] are unknown at
    # threshold 0.5 -> per-instance bounds 97..100 of 100.
    await _inject(servers["b"], 0.2, 97)
    await _inject(servers["b"], 0.4, 3)

    # Wait until a scrape window has captured the injected traffic on both
    # instances, then the next check evaluates that window.
    deadline = time.monotonic() + 8
    captured = False
    async with httpx.AsyncClient() as client:
        while time.monotonic() < deadline:
            stats = (await client.get(f"{servers['verifier']}/stats")).json()
            windows = stats["current_ready_samples"]
            counts = [v.get("requests_in_window", 0) for v in windows.values()]
            if counts == [100, 100]:
                captured = True
                break
            await asyncio.sleep(0.1)
    assert captured, "injected traffic never appeared in a scrape window"

    result = await _check(servers["verifier"], threshold=0.5, max_wait=8)
    assert result["decision"] == "inconclusive", result
    b = result["instances"]["sample-b"]
    assert b["compliant_lower"] == 97
    assert b["compliant_upper"] == 100
    # combined: lower 196/200 = 98%, upper 199/200 = 99.5% -> straddles 99%
    assert result["lower_ratio"] == pytest.approx(0.98)
    assert result["upper_ratio"] == pytest.approx(0.995)


async def test_zero_traffic_is_inconclusive(servers):
    await _wait_ready(servers["verifier"])
    result = await _check(servers["verifier"], threshold=0.5, max_wait=8)
    assert result["decision"] == "inconclusive", result
    assert result["zero_traffic"] is True
    assert result["total_requests"] == 0
    assert "零流量" in result["reason"]


async def test_restart_invalidates_samples_then_recovers(servers):
    await _inject(servers["a"], 0.2, 50)
    await _inject(servers["b"], 0.2, 50)
    await _wait_ready(servers["verifier"])

    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{servers['a']}/restart")
        new_run_id = resp.json()["run_id"]

        # Wait until the verifier has observed the new process exactly once:
        # the newest two scrapes then straddle the restart (different run
        # ids), which must be rejected.
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            stats = (await client.get(f"{servers['verifier']}/stats")).json()
            info = stats["instances"]["sample-a"]
            ready = stats["current_ready_samples"]["sample-a"]
            if (
                info["last_process_id"] == new_run_id
                and ready.get("not_ready_reason") == "process_restarted"
            ):
                break
            await asyncio.sleep(0.05)
        else:
            raise AssertionError("verifier never observed the restart boundary")

        # A check with a short deadline must return sample-a as missing
        # evidence rather than silently using cross-process samples.
        check_resp = await client.get(
            f"{servers['verifier']}/check",
            params={"threshold": 0.5, "max-wait": 0.5},
        )
        result = check_resp.json()
    assert result["timed_out"] is True
    assert "sample-a" in result["missing_instances"]
    assert result["decision"] == "inconclusive"
    assert "不同进程" in result["missing_reasons"]["sample-a"]

    # After two fresh same-process scrapes (~2.1s) the store recovers.
    await _wait_ready(servers["verifier"], timeout=8)
    stats = await _get_stats(servers["verifier"])
    ready_a = stats["current_ready_samples"]["sample-a"]
    assert ready_a["process_id"] == new_run_id
    assert ready_a["requests_in_window"] == 0

    result = await _check(servers["verifier"], threshold=0.5, max_wait=8)
    assert result["decision"] == "inconclusive"  # zero traffic after restart
    assert result["instances"]["sample-a"]["process_id"] == new_run_id


async def test_missing_instance_times_out_with_reason(monkeypatch, servers):
    # Shut instance B down; the verifier keeps serving and the check returns
    # with B named as the missing-evidence instance.
    async with httpx.AsyncClient() as client:
        # nothing to shut via fixture; simulate by pointing a fresh verifier
        # at a dead port.
        pass
    monkeypatch.setenv(
        "SCRAPE_TARGETS",
        f"sample-a=http://127.0.0.1:{PORTS['a']}/metrics,"
        "sample-b=http://127.0.0.1:9999/metrics",
    )
    extra = Server("verifier2", 9210, create_app)
    await extra.start()
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                "http://127.0.0.1:9210/check",
                params={"threshold": 0.5, "max-wait": 3},
            )
            result = resp.json()
        assert result["timed_out"] is True
        assert result["missing_instances"] == ["sample-b"]
        assert result["missing_reasons"]["sample-b"] == "成功抓取不足两次"
    finally:
        await extra.stop()
        monkeypatch.delenv("SCRAPE_TARGETS", raising=False)


async def test_concurrent_checks_share_samples_and_isolate_timeouts(servers):
    await _inject(servers["a"], 0.2, 99)
    await _inject(servers["a"], 2.0, 1)
    await _inject(servers["b"], 0.2, 99)
    await _inject(servers["b"], 2.0, 1)

    async with httpx.AsyncClient(timeout=30) as client:
        # One impatient request (likely times out) alongside a patient one;
        # both must end independently and the patient one must still decide.
        short = client.get(
            f"{servers['verifier']}/check",
            params={"threshold": 0.5, "max-wait": 0.2},
        )
        long = client.get(
            f"{servers['verifier']}/check",
            params={"threshold": 0.5, "max-wait": 8},
        )
        short_resp, long_resp = await asyncio.gather(short, long)

    short_result = short_resp.json()
    long_result = long_resp.json()
    assert long_result["decision"] == "proceed"
    # The impatient request either timed out or (if samples were already
    # cached from another test) succeeded — but it never broke the long one.
    assert short_result["decision"] in ("inconclusive", "proceed")

    stats = await _get_stats(servers["verifier"])
    # Two instances scraped in lockstep; attempts must be shared, never
    # multiplied per concurrent check request.
    for name in ("sample-a", "sample-b"):
        assert stats["instances"][name]["scrape_attempts"] >= 2


async def test_stats_reports_scrape_counts_and_sample_times(servers):
    await _wait_ready(servers["verifier"])
    stats = await _get_stats(servers["verifier"])
    for name in ("sample-a", "sample-b"):
        info = stats["instances"][name]
        assert info["scrape_attempts"] >= 2
        assert info["scrape_successes"] == info["scrape_attempts"]
        assert info["scrape_failures"] == 0
        sample = stats["current_ready_samples"][name]
        assert sample["gap_seconds"] >= 1.0
        assert sample["earlier_scrape_at"] <= time.time()
        assert sample["later_scrape_at"] <= time.time()


async def _get_stats(base: str) -> dict:
    async with httpx.AsyncClient() as client:
        return (await client.get(f"{base}/stats")).json()


async def _post_buckets(base: str, boundaries: list[float]) -> httpx.Response:
    async with httpx.AsyncClient(timeout=15) as client:
        return await client.post(f"{base}/buckets", json={"boundaries": boundaries})


async def _post_instances(
    base: str, boundaries: list[float], instance: str
) -> httpx.Response:
    async with httpx.AsyncClient(timeout=15) as client:
        return await client.post(
            f"{base}/buckets",
            params={"instance": instance},
            json={"boundaries": boundaries},
        )


def _buckets_url(servers: dict[str, str]) -> str:
    return f"{servers['verifier']}/buckets"


# --------------------------------------------------------------------------- #
# Bucket-table switches
# --------------------------------------------------------------------------- #


async def test_invalid_bucket_submission_keeps_current_table(servers):
    # Invalid tables are rejected and the running table stays in force.
    # ``inf`` / ``nan`` cannot be represented in JSON requests; they are
    # covered by the unit tests for validate_boundaries instead.
    for bad in ([], [0.2, 0.1], [0.1, 0.1], [0.0, 0.5], [-1, 0.5]):
        resp = await _post_buckets(servers["verifier"], bad)
        assert resp.status_code == 400, (bad, resp.text)
        assert "桶" in resp.json()["detail"]

    async with httpx.AsyncClient() as client:
        state = (await client.get(f"{servers['a']}/state")).json()
    assert state["boundaries"] == [float(x) for x in BUCKETS_A.split(",")]
    assert state["switch_count"] == 0

    # An invalid table must not have wedged the switch gate: a valid one is
    # still accepted right away.
    ok = await _post_buckets(servers["verifier"], [0.2, 0.6, 1.2])
    assert ok.status_code == 200, ok.text
    assert ok.json()["all_confirmed"] is True

    # Only sample-a gets this table; sample-b keeps its previous one.
    only_a = await _post_instances(
        servers["verifier"], [0.3, 0.9, 1.8], instance="sample-a"
    )
    assert only_a.status_code == 200, only_a.text
    assert only_a.json()["all_confirmed"] is True
    async with httpx.AsyncClient() as client:
        state_b = (await client.get(f"{servers['b']}/buckets")).json()
    assert state_b["current_table"]["boundaries"] == [0.2, 0.6, 1.2]

    unknown = await _post_instances(
        servers["verifier"], [0.3, 0.9], instance="nope"
    )
    assert unknown.status_code == 404


async def test_check_across_a_switch_uses_old_and_new_tables(servers):
    """A single check window holds traffic counted before *and* after a switch.

    The switch is applied by an explicit ``GET /metrics`` from the test, so
    both segments of the verifier's next window carry traffic.
    """

    async def _attempt() -> dict | None:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(f"{servers['a']}/restart")
            await client.post(f"{servers['b']}/restart")
        await _wait_ready(servers["verifier"])
        await _wait_windows(servers["verifier"], {"sample-a": 0, "sample-b": 0})

        # Anchor to a scrape that has just completed, so the next verifier
        # scrape (~1.05s later) is safely after all traffic below.
        await _wait_fresh_scrape(servers["verifier"], ("sample-a", "sample-b"))

        # Old-table traffic inside the window.
        for base in (servers["a"], servers["b"]):
            await _inject(base, 0.2, 20)
            await _inject(base, 2.0, 5)

        # Freeze the old table (switch applies on this very response) ...
        async with httpx.AsyncClient(timeout=10) as client:
            for base in (servers["a"], servers["b"]):
                resp = await client.post(
                    f"{base}/buckets", json={"boundaries": [0.15, 0.4, 0.9, 2.0, 4.0]}
                )
                assert resp.status_code == 200, resp.text
                await client.get(f"{base}/metrics")

        # ... then new-table traffic before the verifier's next scrape.
        for base in (servers["a"], servers["b"]):
            await _inject(base, 0.2, 30)
            await _inject(base, 0.6, 10)
            await _inject(base, 3.0, 5)

        deadline = time.monotonic() + 0.9
        async with httpx.AsyncClient() as client:
            while time.monotonic() < deadline:
                stats = (await client.get(f"{servers['verifier']}/stats")).json()
                samples = stats["current_ready_samples"]
                if all(
                    isinstance(v, dict) and v.get("crossed_bucket_change")
                    for v in samples.values()
                ):
                    return samples
                await asyncio.sleep(0.02)
        return None

    straddling = None
    for _ in range(4):
        straddling = await _attempt()
        if straddling is not None:
            break
    assert straddling is not None, "no scrape window straddled the switch"

    result = await _check(servers["verifier"], threshold=0.5, max_wait=1)
    assert result["crossed_bucket_change"] is True
    for name in ("sample-a", "sample-b"):
        sample = result["instances"][name]
        assert sample["crossed_bucket_change"] is True
        assert sample["requests_in_window"] == 70
        # old table: 20 known-good, 5 known-slow -> exactly 20
        # new table: 30 <= 0.4 known-good, 10 in (0.4, 0.9] unknown -> 30..40
        assert sample["compliant_lower"] == 50
        assert sample["compliant_upper"] == 60
        assert len(sample["segments"]) == 2
        old, new = sample["segments"]
        assert old["table_id"] != new["table_id"]
        assert old["requests_in_window"] == 25
        assert old["compliant_lower"] == old["compliant_upper"] == 20
        assert new["requests_in_window"] == 45
        assert (new["compliant_lower"], new["compliant_upper"]) == (30, 40)
        assert new["from_zero_baseline"] is True

    assert result["total_requests"] == 140
    assert result["compliant_lower"] == 100
    assert result["compliant_upper"] == 120
    assert result["decision"] == "halt", result


async def test_switch_is_applied_at_the_next_scrape_and_rejects_a_second(servers):
    # Use the raw target endpoints to observe the machine-level contract.
    async with httpx.AsyncClient(timeout=10) as client:
        before = (await client.get(f"{servers['a']}/state")).json()
        resp = await client.post(
            f"{servers['a']}/buckets", json={"boundaries": [0.2, 1.0]}
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["status"] == "pending"

        # Nothing changed yet: the switch takes effect at the next scrape.
        assert (await client.get(f"{servers['a']}/state")).json()["boundaries"] == before["boundaries"]

        # A second submission before the next scrape is refused.
        again = await client.post(
            f"{servers['a']}/buckets", json={"boundaries": [0.3, 1.0]}
        )
        assert again.status_code == 409

        await client.get(f"{servers['a']}/metrics")
        after = (await client.get(f"{servers['a']}/state")).json()
    assert after["boundaries"] == [0.2, 1.0]
    assert after["table_id"] != before["table_id"]
    assert after["retro_table_id"] == before["table_id"]
    assert after["pending_boundaries"] is None
    assert after["switch_count"] == before["switch_count"] + 1


async def test_injections_around_a_switch_are_counted_once(servers):
    async with httpx.AsyncClient(timeout=10) as client:
        await client.post(f"{servers['b']}/restart")
        state = (await client.get(f"{servers['b']}/state")).json()
        old_table = state["table_id"]

        # Injection before the switch lands in the old table.
        await client.post(
            f"{servers['b']}/inject",
            json={"duration_seconds": 0.05, "count": 7},
        )
        await client.post(
            f"{servers['b']}/buckets", json={"boundaries": [0.2, 1.0]}
        )
        # Injection after the switch is queued but not yet applied: it still
        # lands in the old table until the next scrape.
        await client.post(
            f"{servers['b']}/inject",
            json={"duration_seconds": 0.05, "count": 3},
        )
        state = (await client.get(f"{servers['b']}/state")).json()
        assert state["count"] == 10
        assert state["table_id"] == old_table

        await client.get(f"{servers['b']}/metrics")  # switch happens here
        await client.post(
            f"{servers['b']}/inject",
            json={"duration_seconds": 0.05, "count": 5},
        )
        state = (await client.get(f"{servers['b']}/state")).json()
        assert state["table_id"] != old_table
        assert state["retro_table_id"] == old_table
        assert state["count"] == 5
        assert state["retro_count"] == 10

        # The exposition carries both tables and the totals add up.
        metrics = (await client.get(f"{servers['b']}/metrics")).text

    assert 'bucket_table="' + state["table_id"] + '"' in metrics
    assert 'retro="true"' in metrics
    assert 'bucket_table="' + old_table + '"' in metrics
