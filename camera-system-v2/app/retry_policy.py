"""
Retry policy: service_config.retry_policy in DB, fallback to env RETRY_*.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Sequence


DEFAULT_MAX_ATTEMPTS = 4
DEFAULT_BACKOFF_SECONDS: tuple[int, ...] = (60, 300, 900, 3600)


def _safe_int(value: Any, default: int | None) -> int | None:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int
    schedule_seconds: tuple[int, ...]


def _parse_env_schedule(*, default: tuple[int, ...] = DEFAULT_BACKOFF_SECONDS) -> tuple[int, ...]:
    raw = os.getenv(
        "RETRY_BACKOFF_SECONDS", ",".join(str(x) for x in (default or DEFAULT_BACKOFF_SECONDS))
    )
    out: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if part:
            try:
                out.append(int(part))
            except (TypeError, ValueError):
                continue
    if out:
        return tuple(out)
    return default or (60,)


def fetch_retry_policy(conn) -> RetryPolicy:
    env_max_attempts = _safe_int(os.getenv("RETRY_MAX_ATTEMPTS"), DEFAULT_MAX_ATTEMPTS)
    env_schedule = _parse_env_schedule(default=DEFAULT_BACKOFF_SECONDS)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT value_json FROM service_config WHERE key = 'retry_policy' LIMIT 1"
        )
        row = cur.fetchone()

    max_a = env_max_attempts if env_max_attempts is not None else DEFAULT_MAX_ATTEMPTS
    sched = env_schedule

    if row and row[0]:
        j: Any = row[0]
        try:
            if isinstance(j, str):
                j = json.loads(j)
        except Exception:
            j = None

        if isinstance(j, dict):
            db_max = _safe_int(j.get("max_attempts"), None)
            if db_max is not None:
                max_a = db_max

            db_sched_raw = j.get("schedule_seconds")
            if isinstance(db_sched_raw, list):
                db_sched: list[int] = []
                for x in db_sched_raw:
                    v = _safe_int(x, None)
                    if v is not None:
                        db_sched.append(v)
                if db_sched:
                    sched = tuple(db_sched)

    return RetryPolicy(int(max_a), tuple(int(x) for x in sched))


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
