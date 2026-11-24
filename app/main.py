"""FastAPI application for the canary rollout SLO verification."""

from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from .buckets import BucketValidationError
from .decisions import DEFAULT_TARGET, Decision, decide
from .histogram import combine_instances, segment_bounds as segment_bounds_for
from .scraper import ScrapeStore, SwitchRejected, store_lifespan


def _instance_urls() -> dict[str, str]:
    raw = os.environ.get(
        "SCRAPE_TARGETS",
        "sample-a=http://sample-a:9101/metrics,sample-b=http://sample-b:9102/metrics",
    )
    instances: dict[str, str] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            raise ValueError(f"invalid SCRAPE_TARGETS entry: {part!r}")
        name, url = part.split("=", 1)
        instances[name.strip()] = url.strip()
    if len(instances) < 2:
        raise ValueError("at least two scrape instances are required")
    return instances


class BucketSubmitBody(BaseModel):
    boundaries: list[float] = Field(
        ...,
        description="严格递增的有限正数桶边界（秒），例如 [0.05, 0.1, 0.5, 1, 5]",
    )


def create_app() -> FastAPI:
    store = ScrapeStore(_instance_urls())

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async with store_lifespan(store):
            yield

    app = FastAPI(
        title="灰度放量核查 API",
        description=(
            "抓取两个本地 Prometheus 延迟直方图实例，每实例取最近两次抓取"
            "（均须在近 3 秒内、来自同一进程、间隔至少 1 秒），用桶计数增量"
            "计算两实例合计达标比例的上下界并给出放量建议。"
            "支持运行期提交新桶表：换桶在样例下一次成功抓取时生效，"
            "跨换桶的窗口按旧表/新表两段分别求界后合并。"
        ),
        version="1.1.0",
        lifespan=lifespan,
    )
    app.state.store = store

    class InstanceSample(BaseModel):
        earlier_scrape_at: float
        later_scrape_at: float
        process_id: str | None
        gap_seconds: float
        requests_in_window: int
        compliant_lower: int
        compliant_upper: int
        table_ids: list[str | None] = []
        crossed_bucket_change: bool = False
        segments: list[dict] = []

    class CheckResponse(BaseModel):
        decision: Decision
        reason: str
        threshold_seconds: float
        target_ratio: float
        waited_seconds: float
        timed_out: bool
        total_requests: int | None = None
        compliant_lower: int | None = None
        compliant_upper: int | None = None
        lower_ratio: float | None = None
        upper_ratio: float | None = None
        zero_traffic: bool | None = None
        crossed_bucket_change: bool = False
        instances: dict[str, InstanceSample] = {}
        missing_instances: list[str] = []
        missing_reasons: dict[str, str] = {}

    @app.get("/check", response_model=CheckResponse, tags=["核查"])
    async def check(
        threshold_seconds: float = Query(
            ...,
            gt=0,
            alias="threshold",
            description="延迟阈值（秒），超过该时长视为不达标",
        ),
        max_wait_seconds: float = Query(
            ...,
            gt=0,
            le=60,
            alias="max-wait",
            description="最长等待样本时间（秒），超时即按缺证据返回",
        ),
        target: float = Query(
            DEFAULT_TARGET, gt=0, le=1, description="达标比例目标，默认 0.99"
        ),
    ) -> CheckResponse:
        started = time.monotonic()
        deadline = started + max_wait_seconds
        # Each request owns its deadline; cancellation of another request
        # never reaches the shared store.
        ready, missing = await store.wait_for_samples(deadline)
        waited = time.monotonic() - started
        timed_out = bool(missing)

        if missing:
            reasons = {name: _missing_reason(store, name) for name in missing}
            return CheckResponse(
                decision=Decision.INCONCLUSIVE,
                reason=(
                    "等待超时，以下实例缺少合格证据："
                    + "、".join(f"{n}（{reasons[n]}）" for n in missing)
                    + "，证据不足。"
                ),
                threshold_seconds=threshold_seconds,
                target_ratio=target,
                waited_seconds=round(waited, 4),
                timed_out=True,
                missing_instances=missing,
                missing_reasons=reasons,
            )

        deltas = {name: sample.delta for name, sample in ready.items()}
        combined = combine_instances(deltas, threshold_seconds)
        decision, reason = decide(combined, target=target)

        instance_payload: dict[str, InstanceSample] = {}
        for name, sample in ready.items():
            bounds = combined.per_instance[name]
            earlier_at = sample.earlier.at
            later_at = sample.later.at
            segments = []
            for segment in sample.delta.segments:
                segment_bounds = segment_bounds_for(segment, threshold_seconds)
                segments.append(
                    {
                        "table_id": segment.table_id,
                        "boundaries": segment.boundaries,
                        "requests_in_window": segment.delta_inf,
                        "compliant_lower": int(segment_bounds.lower),
                        "compliant_upper": int(segment_bounds.upper),
                        "from_zero_baseline": segment.from_zero_baseline,
                    }
                )
            instance_payload[name] = InstanceSample(
                earlier_scrape_at=earlier_at,
                later_scrape_at=later_at,
                process_id=sample.later.snapshot.process_id,
                gap_seconds=round(later_at - earlier_at, 4),
                requests_in_window=sample.delta.total_requests,
                compliant_lower=int(bounds.lower),
                compliant_upper=int(bounds.upper),
                table_ids=sample.table_ids,
                crossed_bucket_change=sample.delta.crosses_table_change,
                segments=segments,
            )

        crossed = any(sample.delta.crosses_table_change for sample in ready.values())
        return CheckResponse(
            decision=decision,
            reason=reason,
            threshold_seconds=threshold_seconds,
            target_ratio=target,
            waited_seconds=round(waited, 4),
            timed_out=False,
            total_requests=combined.total_requests,
            compliant_lower=combined.compliant_lower,
            compliant_upper=combined.compliant_upper,
            lower_ratio=combined.lower_ratio,
            upper_ratio=combined.upper_ratio,
            zero_traffic=combined.is_zero_traffic,
            crossed_bucket_change=crossed,
            instances=instance_payload,
        )

    @app.get("/stats", tags=["核查"])
    async def stats() -> dict:
        """Actual scrape counts and the timestamps of samples in use."""
        data = store.stats()
        now = time.time()
        data["checked_at"] = now
        data["config"] = {
            "min_sample_gap_seconds": store.min_gap,
            "freshness_window_seconds": store.freshness,
            "scrape_interval_seconds": store.scrape_interval,
        }
        data["current_ready_samples"] = {}
        for name in store.instances:
            outcome = store.sample_status(name, now)
            if hasattr(outcome, "earlier"):
                data["current_ready_samples"][name] = {
                    "earlier_scrape_at": outcome.earlier.at,
                    "later_scrape_at": outcome.later.at,
                    "process_id": outcome.later.snapshot.process_id,
                    "gap_seconds": round(outcome.later.at - outcome.earlier.at, 4),
                    "requests_in_window": outcome.delta.total_requests,
                    "table_ids": outcome.table_ids,
                    "crossed_bucket_change": outcome.delta.crosses_table_change,
                }
            else:
                data["current_ready_samples"][name] = {"not_ready_reason": outcome}
        return data

    @app.get("/health", tags=["运维"])
    async def health() -> dict:
        return {"ok": True}

    @app.post("/buckets", tags=["换桶"])
    async def submit_buckets(
        body: BucketSubmitBody,
        instance: str | None = Query(
            None,
            description="只向该实例提交；省略则向所有实例提交同一份桶表",
        ),
    ) -> dict:
        """提交新桶表，并在下一次成功抓取后确认换桶完成。

        规则：

        * 桶边界必须是严格递增的有限正数，否则返回 400，原桶表保留；
        * 换桶最迟在一次成功抓取时生效，注入照常计入；
        * 上一份新桶表尚未出现在抓取中时再次提交会被拒绝（409）；
        * 每笔请求只按当时生效的桶表计入一次。
        """
        if instance is not None and instance not in store.instances:
            raise HTTPException(
                status_code=404, detail=f"未知实例 {instance!r}"
            )
        names = [instance] if instance else list(store.instances)
        try:
            if instance:
                accepted = [
                    await store.submit_bucket_table(instance, body.boundaries)
                ]
            else:
                accepted = await store.submit_bucket_table_all(
                    names, body.boundaries
                )
        except BucketValidationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except SwitchRejected as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

        # Wait (bounded) for the new table to actually show up in a scrape so
        # the response reports real progress instead of "hopefully queued".
        # All instances are waited on concurrently, under one shared deadline.
        timeout = float(os.environ.get("SWITCH_CONFIRM_TIMEOUT", "5"))
        deadline = time.monotonic() + timeout
        snapshots = await asyncio.gather(
            *(
                store.wait_until_table_changed(
                    item["instance"], item["previous_table_id"], deadline
                )
                for item in accepted
            )
        )
        progress: dict[str, dict] = {}
        for item, snapshot in zip(accepted, snapshots):
            name = item["instance"]
            state = store.instances[name]
            progress[name] = {
                "confirmed": snapshot is not None,
                "previous_table_id": item["previous_table_id"],
                "received_table_id": (
                    snapshot.table_id if snapshot is not None else None
                ),
                "submitted_boundaries": item["boundaries"],
                "target_response": item["target_response"],
                "stats": store.stats()["instances"][name],
            }
        return {
            "accepted": True,
            "all_confirmed": all(p["confirmed"] for p in progress.values()),
            "boundaries": body.boundaries,
            "instances": progress,
            "note": (
                "换桶已提交并在下一次成功抓取时生效；"
                "pending 状态会在新桶表被核查器抓取到后结束。"
            ),
        }

    @app.get("/buckets", tags=["换桶"])
    async def get_buckets() -> dict:
        """查看每个实例的换桶进度（待生效 / 已收到的桶表）。"""
        progress: dict[str, dict] = {}
        target_statuses = await asyncio.gather(
            *(store.fetch_target_switch_status(name) for name in store.instances)
        )
        for (name, state), target_progress in zip(
            store.instances.items(), target_statuses
        ):
            latest = state.latest
            snapshot = latest.snapshot if latest is not None and latest.ok else None
            progress[name] = {
                "current_table_id": snapshot.table_id if snapshot else None,
                "current_boundaries": (
                    [b for b in snapshot.buckets if b != float("inf")]
                    if snapshot
                    else None
                ),
                "has_retro_table": bool(snapshot and snapshot.previous is not None),
                "submitted_table": state.submitted_table,
                "submitted_from_table": state.submitted_from_table,
                "submitted_boundaries": state.submitted_boundaries,
                "submitted_at": state.submitted_at,
                "switch_pending": state.submitted_table == "pending",
                "target": target_progress,
            }
        return {"instances": progress}

    return app


def _missing_reason(store: ScrapeStore, name: str) -> str:
    outcome = store.sample_status(name)
    if isinstance(outcome, str):
        return _REASON_TEXT.get(outcome, outcome)
    return "ready"


_REASON_TEXT = {
    "insufficient_scrapes": "成功抓取不足两次",
    "stale_scrape": "最近抓取超出近 3 秒新鲜窗口",
    "samples_too_close": "两次抓取间隔不足 1 秒",
    "process_restarted": "两次抓取来自不同进程（疑似重启）",
    "bucket_table_changed": "两次抓取之间桶表已切换，且缺少可衔接的旧桶计数",
}


app = create_app()
