"""Singleton factory for the active queue backend."""
from __future__ import annotations

import logging
import os
import threading
from typing import Callable, Optional

from .backends import (
    QUEUE_NAME_DEFAULT,
    DistributedQueueBackend,
    InMemoryQueueBackend,
    QueueSaturatedError,
    RedisQueueBackend,
    _queue_capacity_limit,
)
from .metrics import QueueMetrics

logger = logging.getLogger(__name__)

_QUEUE_INSTANCE: Optional[DistributedQueueBackend] = None
_QUEUE_METRICS: Optional[QueueMetrics] = None
_QUEUE_ON_BACKEND_CHANGE: Optional[Callable[[str, dict], None]] = None
_QUEUE_LOCK = threading.Lock()


def _is_test_mode() -> bool:
    for var in ("TESTING", "TEST_MODE"):
        if os.getenv(var, "false").lower() in {"1", "true", "yes"}:
            return True
    return False


def _is_production() -> bool:
    return os.getenv("ENV", "").lower() in {"production", "prod"}


def _require_redis() -> bool:
    return os.getenv("NODUS_REQUIRE_REDIS", os.getenv("AINDY_REQUIRE_REDIS", "false")).lower() in {
        "1", "true", "yes"
    }


def _fallback_to_memory(
    exc: Exception,
    *,
    metrics: QueueMetrics,
    on_backend_change: Optional[Callable[[str, dict], None]],
    queue_name: str,
) -> InMemoryQueueBackend:
    if _require_redis():
        raise RuntimeError(
            f"NODUS_REQUIRE_REDIS=true but Redis is unavailable: {exc}. "
            "Set NODUS_REQUIRE_REDIS=false to allow in-memory fallback."
        ) from exc

    metrics.on_fallback()
    logger.warning(
        "[Queue] Redis unavailable (%s) — falling back to in-memory queue. "
        "In multi-instance mode jobs will NOT be shared across instances. "
        "Set NODUS_REQUIRE_REDIS=true to prevent degraded-mode startup.",
        exc,
    )
    if on_backend_change is not None:
        try:
            on_backend_change("degraded", {"reason": str(exc), "fallback": "memory"})
        except Exception:
            pass
    return InMemoryQueueBackend(
        metrics=metrics,
        degraded=True,
        fallback_reason=str(exc),
    )


def get_queue(
    *,
    force_memory: bool = False,
    metrics: Optional[QueueMetrics] = None,
    on_backend_change: Optional[Callable[[str, dict], None]] = None,
) -> DistributedQueueBackend:
    """Return the process-level singleton queue backend.

    Selection order
    ---------------
    1. ``force_memory=True`` → fresh ``InMemoryQueueBackend`` (not cached).
    2. ``TESTING=1`` / ``TEST_MODE=1`` → ``InMemoryQueueBackend`` (cached).
    3. ``REDIS_URL`` is set → ``RedisQueueBackend``.
    4. Fallback → ``InMemoryQueueBackend`` with a warning.

    Args:
        force_memory: Return a fresh in-memory backend, bypassing the singleton.
            Use in tests that need isolated queues.
        metrics: Optional ``QueueMetrics`` hook.  Ignored after the singleton is
            created; pass it on the first call.
        on_backend_change: Optional callback fired when the backend changes mode
            (e.g. Redis → memory fallback or reconnect).  Signature:
            ``fn(event: str, payload: dict) -> None``.

    Call ``reset_queue()`` between tests to get a clean instance.
    """
    global _QUEUE_INSTANCE, _QUEUE_METRICS, _QUEUE_ON_BACKEND_CHANGE

    _metrics = metrics or QueueMetrics()

    if force_memory:
        backend = InMemoryQueueBackend(metrics=_metrics)
        _metrics.on_backend_mode_changed(False)
        return backend

    if _QUEUE_INSTANCE is not None:
        return _QUEUE_INSTANCE

    with _QUEUE_LOCK:
        if _QUEUE_INSTANCE is not None:
            return _QUEUE_INSTANCE

        _QUEUE_METRICS = _metrics
        _QUEUE_ON_BACKEND_CHANGE = on_backend_change

        if _is_test_mode():
            _QUEUE_INSTANCE = InMemoryQueueBackend(metrics=_metrics)
            _metrics.on_backend_mode_changed(False)
            return _QUEUE_INSTANCE

        redis_url = os.getenv("REDIS_URL", "")
        queue_name = os.getenv("NODUS_QUEUE_NAME", os.getenv("AINDY_QUEUE_NAME", QUEUE_NAME_DEFAULT))

        if _is_production() and not redis_url:
            raise RuntimeError(
                "Production deployments require RedisQueueBackend for job durability. "
                "Set REDIS_URL before startup."
            )

        if redis_url:
            try:
                candidate = RedisQueueBackend(
                    url=redis_url,
                    queue_name=queue_name,
                    metrics=_metrics,
                )
                candidate.assert_ready()
                _QUEUE_INSTANCE = candidate
                logger.info("[Queue] Redis backend url=%s queue=%s", redis_url, queue_name)
            except Exception as exc:
                _QUEUE_INSTANCE = _fallback_to_memory(
                    exc,
                    metrics=_metrics,
                    on_backend_change=on_backend_change,
                    queue_name=queue_name,
                )
        else:
            if os.getenv("EXECUTION_MODE", "thread").lower() == "distributed":
                raise RuntimeError(
                    "EXECUTION_MODE=distributed requires REDIS_URL. "
                    "Jobs would be lost on process restart with an in-memory queue. "
                    "Set REDIS_URL or switch to EXECUTION_MODE=thread."
                )
            logger.warning(
                "[Queue] REDIS_URL not set — using in-memory queue. "
                "Multi-process distributed execution requires Redis."
            )
            _QUEUE_INSTANCE = InMemoryQueueBackend(metrics=_metrics)

        _metrics.on_backend_mode_changed(_QUEUE_INSTANCE.backend_name == "redis")
        _metrics.on_snapshot(_QUEUE_INSTANCE.backend_name, _QUEUE_INSTANCE.get_metrics())
        return _QUEUE_INSTANCE


def reset_queue() -> None:
    """Reset the singleton to None.

    Call in test teardown (or after changing ``REDIS_URL``) to force
    re-initialisation on the next ``get_queue()`` call.
    """
    global _QUEUE_INSTANCE
    with _QUEUE_LOCK:
        _QUEUE_INSTANCE = None


def validate_queue_backend() -> DistributedQueueBackend:
    """Fail fast when the configured queue backend is unavailable."""
    backend = get_queue()
    if backend.backend_name == "redis":
        backend.assert_ready()
    if _QUEUE_METRICS:
        _QUEUE_METRICS.on_snapshot(backend.backend_name, backend.get_metrics())
        _QUEUE_METRICS.on_backend_mode_changed(backend.backend_name == "redis")
    return backend


def get_queue_health_snapshot() -> dict:
    """Return a health dict for the active backend, including fallback reason."""
    backend = get_queue()
    snapshot = backend.health_snapshot()
    snapshot["reason"] = backend.fallback_reason
    return snapshot


def attempt_queue_backend_reconnect() -> bool:
    """Try to promote a degraded in-memory backend back to Redis.

    Returns True if the reconnect succeeded and the singleton was replaced.
    No-op when the current backend is already Redis or Redis is not configured.
    """
    global _QUEUE_INSTANCE

    redis_url = os.getenv("REDIS_URL", "")
    if not redis_url:
        return False

    with _QUEUE_LOCK:
        current = _QUEUE_INSTANCE
        if current is None or current.backend_name == "redis" or not current.degraded:
            return False

        queue_name = os.getenv("NODUS_QUEUE_NAME", os.getenv("AINDY_QUEUE_NAME", QUEUE_NAME_DEFAULT))
        _metrics = _QUEUE_METRICS or QueueMetrics()
        try:
            candidate = RedisQueueBackend(
                url=redis_url,
                queue_name=queue_name,
                metrics=_metrics,
            )
            candidate.assert_ready()
        except Exception as exc:
            logger.debug("[Queue] Redis reconnect attempt failed: %s", exc)
            return False

        _QUEUE_INSTANCE = candidate

    _metrics.on_backend_mode_changed(True)
    _metrics.on_snapshot(candidate.backend_name, candidate.get_metrics())

    if _QUEUE_ON_BACKEND_CHANGE is not None:
        try:
            _QUEUE_ON_BACKEND_CHANGE("recovered", {"backend": "redis"})
        except Exception:
            pass

    logger.info("[Queue] Redis connection restored — queue backend switched to Redis.")
    return True
