"""
Retry policy: service_config.retry_policy in DB, fallback to env RETRY_*.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Sequence


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int
    schedule_seconds: tuple[int, ...]


def _parse_env_schedule() -> tuple[int, ...]:
    raw = os.getenv("RETRY_BACKOFF_SECONDS", "60,300,900,3600")
    out: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if part:
            out.append(int(part))
    return tuple(out) if out else (60,)


def fetch_retry_policy(conn) -> RetryPolicy:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT value_json FROM service_config WHERE key = 'retry_policy' LIMIT 1"
        )
        row = cur.fetchone()
    if row and row[0]:
        j: dict[str, Any] = row[0]
        if isinstance(j, str):
            j = json.loads(j)
        max_a = int(j.get("max_attempts", 6))
        sched = j.get("schedule_seconds") or []
        if isinstance(sched, list) and sched:
            return RetryPolicy(
                max_a, tuple(int(x) for x in sched if x is not None)
            )
    max_a = int(os.getenv("RETRY_MAX_ATTEMPTS", "4"))
    return RetryPolicy(max_a, _parse_env_schedule())


def delay_before_next_retry(attempt_count_after_failure: int, schedule: Sequence[int]) -> int:
    """
    attempt_count_after_failure — значение attempt_count после неудачной попытки processing.
    Индекс задержки: attempt_count_after_failure - 1, с насыщением по последнему шагу.
    """
    if not schedule:
        return 60
    if attempt_count_after_failure <= 0:
        return int(schedule[0])
    idx = min(attempt_count_after_failure - 1, len(schedule) - 1)
    return int(schedule[idx])
