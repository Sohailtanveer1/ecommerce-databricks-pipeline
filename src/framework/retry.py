"""
Activity-level retry with exponential backoff.

Two layers of retry in this platform (mirrors the interview answer):
  * **Activity level** (this module): a job/step retries a transient failure
    itself, in-process, before giving up — e.g. a flaky JDBC connection.
  * **Pipeline level** (ADF trigger retry + control.pipeline_runs): a failed run
    is retried by the orchestrator, and reprocesses ONLY the objects marked
    FAILED in the run log — not the whole batch.

`retryable` marks which exceptions are worth retrying (transient) vs fatal
(schema mismatch, auth) which should fail fast.
"""

from __future__ import annotations

import functools
import time

# Transient error signatures worth retrying; extend per connector.
TRANSIENT_MARKERS = (
    "timeout",
    "timed out",
    "connection reset",
    "temporarily unavailable",
    "429",
    "503",
    "throttl",
    "deadlock",
    "could not connect",
    "broken pipe",
)


def is_transient(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return any(m in msg for m in TRANSIENT_MARKERS)


def with_retry(
    max_attempts: int = 3,
    base_delay: float = 2.0,
    max_delay: float = 30.0,
    only_transient: bool = True,
):
    """Decorator: retry with exponential backoff + jitter.

    Fatal (non-transient) errors are re-raised immediately when only_transient=True
    so we fail fast on real bugs instead of hammering."""

    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            attempt = 0
            while True:
                attempt += 1
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001 - deliberate retry boundary
                    fatal = only_transient and not is_transient(exc)
                    if fatal or attempt >= max_attempts:
                        raise
                    delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
                    # simple deterministic jitter (avoids importing random in jobs)
                    delay += (hash((fn.__name__, attempt)) % 1000) / 1000.0
                    print(
                        f"[retry] {fn.__name__} attempt {attempt} failed: {exc} "
                        f"-> retrying in {delay:.1f}s"
                    )
                    time.sleep(delay)

        return wrapper

    return deco
