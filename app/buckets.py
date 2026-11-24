"""Validation shared by the sample targets and the verifier.

A bucket table is a non-empty sequence of strictly increasing, finite,
positive upper bounds.  Anything else is rejected and the target keeps the
table it was using before the invalid submission.
"""

from __future__ import annotations

import math
from collections.abc import Iterable


class BucketValidationError(ValueError):
    """The submitted bucket boundaries are not a usable table."""


def validate_boundaries(values: Iterable[float]) -> list[float]:
    """Return a normalised copy of ``values`` or raise.

    Rules (from the operator-facing contract):

    * at least one boundary;
    * every boundary is finite (``NaN`` / ``+-Inf`` rejected);
    * every boundary is strictly positive;
    * values are strictly increasing.
    """

    try:
        raw_values = list(values)
    except TypeError as exc:  # pragma: no cover - defensive
        raise BucketValidationError("桶边界必须是数字列表") from exc

    if not raw_values:
        raise BucketValidationError("桶边界不能为空")

    result: list[float] = []
    previous: float | None = None
    for index, raw in enumerate(raw_values):
        try:
            value = float(raw)
        except (TypeError, ValueError) as exc:
            raise BucketValidationError(f"第 {index + 1} 个桶边界不是数字: {raw!r}") from exc
        if not math.isfinite(value):
            raise BucketValidationError(f"第 {index + 1} 个桶边界必须是有限数: {value!r}")
        if value <= 0:
            raise BucketValidationError(f"第 {index + 1} 个桶边界必须为正数: {value!r}")
        if previous is not None and value <= previous:
            raise BucketValidationError(
                "桶边界必须严格递增: "
                f"第 {index + 1} 个值 {value!r} 不大于前一个值 {previous!r}"
            )
        result.append(value)
        previous = value

    return result
