"""DistributedQueueBackend, RedisQueueBackend, InMemoryQueueBackend."""
from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Optional

import tenacity

from .metrics import QueueMetrics
from .payload import QueueJobPayload

logger = logging.getLogger(__name__)

QUEUE_NAME_DEFAULT = "nodus:jobs"


# ---------------------------------------------------------------------------
# Saturated error
# ---------------------------------------------------------------------------

class QueueSaturatedError(RuntimeError):
    """Raised when the queue rejects work due to reaching its capacity limit."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int = 503,
        retry_after_seconds: int = 5,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds


# ---------------------------------------------------------------------------
# Capacity helpers (env-var based — no config object dependency)
# ---------------------------------------------------------------------------

def _queue_capacity_limit() -> int:
    for name in ("NODUS_QUEUE_MAXSIZE", "MAX_QUEUE_SIZE", "AINDY_ASYNC_QUEUE_MAXSIZE"):
        raw = os.getenv(name)
        if raw is not None:
            try:
                return max(1, int(raw))
            except (TypeError, ValueError):
                pass
    return 100


def _saturation_threshold() -> int:
    cap = _queue_capacity_limit()
    raw = os.getenv("NODUS_QUEUE_SATURATION_THRESHOLD", "")
    if raw:
        try:
            return max(1, min(int(raw), cap))
        except (TypeError, ValueError):
            pass
    return cap


# ---------------------------------------------------------------------------
# Redis retry decorator
# ---------------------------------------------------------------------------

def _log_redis_retry(retry_state: tenacity.RetryCallState) -> None:
    if retry_state.outcome is None:
        return
    exc = retry_state.outcome.exception()
    if exc is None:
        return
    logger.warning(
        "RedisQueueBackend: retry attempt=%s exception=%s",
        retry_state.attempt_number,
        exc,
    )


def _redis_retry():
    """Retry transient Redis failures with bounded exponential backoff."""
    import redis  # noqa: PLC0415

    return tenacity.retry(
        retry=tenacity.retry_if_exception_type(
            (redis.ConnectionError, redis.TimeoutError, redis.BusyLoadingError)
        ),
        wait=tenacity.wait_exponential(multiplier=2, min=0.1, max=2.0),
        stop=tenacity.stop_after_attempt(3),
        before_sleep=_log_redis_retry,
        reraise=True,
    )


# ---------------------------------------------------------------------------
# Abstract interface
# ---------------------------------------------------------------------------

class DistributedQueueBackend(ABC):
    """Abstract queue transport.

    Required methods: ``enqueue``, ``dequeue``, ``ack``, ``fail``, ``get_dlq_depth``.
    Optional overrides: ``enqueue_delayed``, ``process_delayed_jobs``,
    ``requeue_stale_jobs``, ``get_metrics``, ``assert_ready``,
    ``remove_dead_letter``, ``drain_dead_letters``.
    """

    @abstractmethod
    def enqueue(self, payload: QueueJobPayload) -> None:
        """Push a job to the tail of the queue."""

    @abstractmethod
    def dequeue(self, timeout: int = 5) -> Optional[QueueJobPayload]:
        """Block up to *timeout* seconds waiting for a job.

        Returns ``None`` when no job arrives within the window.  The returned
        job is added to the in-flight store so ``requeue_stale_jobs`` can
        recover it if the worker crashes.
        """

    @abstractmethod
    def ack(self, job_id: str) -> None:
        """Mark a job as successfully completed; remove from in-flight."""

    @abstractmethod
    def fail(self, job_id: str, error: str = "") -> None:
        """Mark a job as terminally failed; move to Dead Letter Queue."""

    @abstractmethod
    def get_dlq_depth(self) -> int:
        """Return the number of dead-lettered jobs."""

    def remove_dead_letter(self, job_id: str) -> bool:
        """Remove one dead-lettered job by job_id."""
        return False

    def drain_dead_letters(self) -> int:
        """Remove all dead-lettered jobs and return the number removed."""
        return 0

    def enqueue_delayed(self, payload: QueueJobPayload, delay_seconds: float) -> None:
        """Schedule a job for future execution.  Default: enqueue immediately."""
        self.enqueue(payload)

    def process_delayed_jobs(self) -> int:
        """Promote delayed jobs whose delay has elapsed.  Default: no-op."""
        return 0

    def requeue_stale_jobs(self, timeout_seconds: int = 300) -> int:
        """Re-enqueue in-flight jobs older than *timeout_seconds*.  Default: no-op."""
        return 0

    def get_metrics(self) -> dict:
        return {
            "queue_depth": 0,
            "in_flight_count": 0,
            "failed_jobs": 0,
            "delayed_jobs": 0,
            "dlq_depth": 0,
            "max_queue_size": _queue_capacity_limit(),
            "total_pending_jobs": 0,
            "saturation_threshold": _saturation_threshold(),
        }

    def assert_ready(self) -> None:
        """Validate backend readiness. Default: no-op."""

    @property
    def backend_name(self) -> str:
        return self.__class__.__name__.replace("QueueBackend", "").lower()

    @property
    def degraded(self) -> bool:
        return False

    @property
    def redis_available(self) -> bool:
        return self.backend_name == "redis"

    @property
    def fallback_reason(self) -> str | None:
        return None

    def health_snapshot(self) -> dict:
        metrics = self.get_metrics()
        return {
            "backend": "redis" if self.backend_name == "redis" else "memory",
            "backend_name": self.backend_name,
            "degraded": self.degraded,
            "redis_available": self.redis_available,
            "metrics": metrics,
            "queue_depth": metrics.get("queue_depth", 0),
            "in_flight_count": metrics.get("in_flight_count", 0),
            "dlq_depth": metrics.get("dlq_depth", metrics.get("failed_jobs", 0)),
            "delayed_jobs": metrics.get("delayed_jobs", 0),
        }


# ---------------------------------------------------------------------------
# Redis backend
# ---------------------------------------------------------------------------

class RedisQueueBackend(DistributedQueueBackend):
    """Redis-backed FIFO queue using LPUSH / BRPOP.

    BRPOP is atomic — only one worker receives each message regardless of how
    many worker processes are running.

    Key layout (``queue_name`` = ``nodus:jobs`` by default)::

        nodus:jobs           — main job list   (LPUSH left / BRPOP right)
        nodus:jobs:inflight  — hash { job_id → {payload, dequeued_at} }
        nodus:jobs:delayed   — sorted set; score = execute_at Unix timestamp
        nodus:jobs:dead      — dead letter list

    Args:
        url: Redis connection URL (e.g. ``redis://localhost:6379/0``).
        queue_name: Key prefix for all queue-related Redis keys.
        max_queue_size: Hard capacity limit.  Defaults to ``NODUS_QUEUE_MAXSIZE``
            env var or 100.
        metrics: Optional ``QueueMetrics`` instance for observability hooks.
        circuit_breaker_threshold: Consecutive Redis failures before the
            circuit opens.
        circuit_breaker_open_seconds: How long to reject calls when open.
    """

    _PROCESS_DELAYED_LUA = """
local ready = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 100)
for _, v in ipairs(ready) do
    redis.call('LPUSH', KEYS[2], v)
    redis.call('ZREM', KEYS[1], v)
end
return #ready
"""

    _ENQUEUE_WITH_CAPACITY_LUA = """
local total = redis.call('LLEN', KEYS[1]) + redis.call('ZCARD', KEYS[2])
if total >= tonumber(ARGV[2]) then
    return -1
end
return redis.call('LPUSH', KEYS[1], ARGV[1])
"""

    _ENQUEUE_DELAYED_WITH_CAPACITY_LUA = """
local total = redis.call('LLEN', KEYS[1]) + redis.call('ZCARD', KEYS[2])
if total >= tonumber(ARGV[3]) then
    return -1
end
return redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
"""

    def __init__(
        self,
        url: str,
        queue_name: str = QUEUE_NAME_DEFAULT,
        max_queue_size: int | None = None,
        metrics: Optional[QueueMetrics] = None,
        circuit_breaker_threshold: int = 5,
        circuit_breaker_open_seconds: float = 30.0,
    ) -> None:
        try:
            import redis  # noqa: PLC0415
        except ImportError as exc:
            raise ImportError(
                "redis package is required for RedisQueueBackend. "
                "Install with: pip install 'nodus-queue[redis]'"
            ) from exc
        self._redis = redis.from_url(url, decode_responses=True, socket_timeout=10)
        self._queue_name = queue_name
        self._max_queue_size = max_queue_size or _queue_capacity_limit()
        self._inflight_key = f"{queue_name}:inflight"
        self._delayed_key = f"{queue_name}:delayed"
        self._dlq_key = f"{queue_name}:dead"
        self._process_delayed = self._redis.register_script(self._PROCESS_DELAYED_LUA)
        self._enqueue_with_capacity = self._redis.register_script(self._ENQUEUE_WITH_CAPACITY_LUA)
        self._enqueue_delayed_with_capacity = self._redis.register_script(
            self._ENQUEUE_DELAYED_WITH_CAPACITY_LUA
        )
        import redis as _redis_module  # noqa: PLC0415
        self._redis_exceptions = (
            _redis_module.ConnectionError,
            _redis_module.TimeoutError,
            _redis_module.BusyLoadingError,
        )
        self._failure_count = 0
        self._open_until = 0.0
        self._circuit_breaker_threshold = circuit_breaker_threshold
        self._circuit_breaker_open_seconds = circuit_breaker_open_seconds
        self._metrics = metrics or QueueMetrics()

    def _check_circuit_breaker(self) -> None:
        import redis  # noqa: PLC0415
        if time.monotonic() < self._open_until:
            raise redis.ConnectionError("Circuit breaker open")

    def _record_success(self) -> None:
        if self._failure_count or self._open_until:
            logger.info("RedisQueueBackend: circuit breaker CLOSED (connection restored)")
        self._failure_count = 0
        self._open_until = 0.0

    def _record_failure(self, exc: Exception) -> None:
        if not isinstance(exc, self._redis_exceptions):
            return
        self._failure_count += 1
        if self._failure_count >= self._circuit_breaker_threshold:
            self._open_until = time.monotonic() + self._circuit_breaker_open_seconds
            logger.error(
                "RedisQueueBackend: circuit breaker OPEN for %.1fs after %d failures",
                self._circuit_breaker_open_seconds,
                self._failure_count,
            )

    def _run_redis_operation(self, operation_name: str, fn):
        @_redis_retry()
        def _call():
            return fn()

        try:
            result = _call()
        except self._redis_exceptions as exc:
            self._record_failure(exc)
            logger.warning(
                "RedisQueueBackend: operation=%s failed error=%s", operation_name, exc
            )
            raise
        self._record_success()
        return result

    def assert_ready(self) -> None:
        self._check_circuit_breaker()
        self._run_redis_operation("ping", lambda: self._redis.ping())

    # ── Core operations ────────────────────────────────────────────────────

    def enqueue(self, payload: QueueJobPayload) -> None:
        self._check_circuit_breaker()
        raw = payload.to_json()
        result = self._run_redis_operation(
            "enqueue",
            lambda: self._enqueue_with_capacity(
                keys=[self._queue_name, self._delayed_key],
                args=[raw, str(self._max_queue_size)],
            ),
        )
        if int(result) == -1:
            self._metrics.on_enqueue(self.backend_name, "rejected")
            raise QueueSaturatedError(
                f"Queue is saturated (capacity={self._max_queue_size}). Retry later."
            )
        self._metrics.on_enqueue(self.backend_name, "accepted")
        logger.debug(
            "[Queue:redis] enqueued job_id=%s task=%s idempotency_key=%s",
            payload.job_id, payload.task_name, payload.idempotency_key,
        )
        self._metrics.on_snapshot(self.backend_name, self.get_metrics())

    def dequeue(self, timeout: int = 5) -> Optional[QueueJobPayload]:
        self._check_circuit_breaker()
        result = self._run_redis_operation(
            "dequeue",
            (lambda: self._redis.rpop(self._queue_name))
            if timeout == 0
            else (lambda: self._redis.brpop(self._queue_name, timeout=timeout)),
        )
        if result is None:
            return None
        raw = result if timeout == 0 else result[1]
        try:
            job = QueueJobPayload.from_json(raw)
        except Exception as exc:
            logger.error("[Queue:redis] deserialise failed: %s — raw=%r", exc, raw[:200])
            return None
        inflight_entry = json.dumps({
            "payload": raw,
            "dequeued_at": datetime.now(timezone.utc).isoformat(),
        })
        self._run_redis_operation(
            "dequeue_inflight_hset",
            lambda: self._redis.hset(self._inflight_key, job.job_id, inflight_entry),
        )
        self._metrics.on_dequeue(self.backend_name)
        self._metrics.on_snapshot(self.backend_name, self.get_metrics())
        return job

    def ack(self, job_id: str) -> None:
        self._run_redis_operation(
            "ack",
            lambda: self._redis.hdel(self._inflight_key, job_id),
        )
        logger.debug("[Queue:redis] ack job_id=%s", job_id)
        self._metrics.on_snapshot(self.backend_name, self.get_metrics())

    def fail(self, job_id: str, error: str = "") -> None:
        inflight_raw = self._run_redis_operation(
            "fail_hget",
            lambda: self._redis.hget(self._inflight_key, job_id),
        )
        self._run_redis_operation(
            "fail_hdel",
            lambda: self._redis.hdel(self._inflight_key, job_id),
        )
        try:
            payload_raw = json.loads(inflight_raw or "{}").get("payload", "")
        except Exception:
            payload_raw = ""
        dlq_entry = json.dumps({
            "job_id": job_id,
            "payload_raw": payload_raw,
            "error": error,
            "failed_at": datetime.now(timezone.utc).isoformat(),
        })
        self._run_redis_operation(
            "fail_lpush_dlq",
            lambda: self._redis.lpush(self._dlq_key, dlq_entry),
        )
        self._metrics.on_failure(self.backend_name, "job")
        self._metrics.on_snapshot(self.backend_name, self.get_metrics())
        logger.warning("[Queue:redis] fail→DLQ job_id=%s error=%s", job_id, error)

    # ── Delayed enqueue ────────────────────────────────────────────────────

    def enqueue_delayed(self, payload: QueueJobPayload, delay_seconds: float) -> None:
        """Schedule *payload* for execution after *delay_seconds*.

        Uses a Redis sorted set; call ``process_delayed_jobs()`` periodically
        to promote ready jobs into the main queue.
        """
        raw = payload.to_json()
        execute_at = datetime.now(timezone.utc).timestamp() + delay_seconds
        result = self._run_redis_operation(
            "enqueue_delayed",
            lambda: self._enqueue_delayed_with_capacity(
                keys=[self._queue_name, self._delayed_key],
                args=[raw, str(execute_at), str(self._max_queue_size)],
            ),
        )
        if int(result) == -1:
            self._metrics.on_enqueue(self.backend_name, "rejected")
            raise QueueSaturatedError(
                f"Queue is saturated (capacity={self._max_queue_size}). Retry later."
            )
        self._metrics.on_enqueue(self.backend_name, "accepted")
        logger.debug(
            "[Queue:redis] delayed enqueue job_id=%s delay=%.1fs",
            payload.job_id, delay_seconds,
        )
        self._metrics.on_snapshot(self.backend_name, self.get_metrics())

    def process_delayed_jobs(self) -> int:
        """Promote all delayed jobs whose execute_at ≤ now into the main queue.

        Uses a Lua script for atomicity.  Returns the number of jobs promoted.
        """
        now_ts = datetime.now(timezone.utc).timestamp()
        count = self._run_redis_operation(
            "process_delayed_jobs",
            lambda: self._process_delayed(
                keys=[self._delayed_key, self._queue_name],
                args=[str(now_ts)],
            ),
        )
        if count:
            logger.info("[Queue:redis] promoted %d delayed jobs", count)
        self._metrics.on_snapshot(self.backend_name, self.get_metrics())
        return int(count)

    # ── Visibility timeout recovery ────────────────────────────────────────

    def requeue_stale_jobs(self, timeout_seconds: int = 300) -> int:
        """Re-enqueue in-flight jobs dequeued more than *timeout_seconds* ago.

        Safe to call from multiple workers concurrently — the first HDEL wins.
        Returns the number of jobs re-enqueued.
        """
        now = datetime.now(timezone.utc)
        entries = self._run_redis_operation(
            "requeue_stale_jobs_hgetall",
            lambda: self._redis.hgetall(self._inflight_key),
        )
        requeued = 0
        for job_id, entry_raw in entries.items():
            try:
                entry = json.loads(entry_raw)
                dequeued_at = datetime.fromisoformat(entry["dequeued_at"])
                age_seconds = (now - dequeued_at).total_seconds()
                if age_seconds <= timeout_seconds:
                    continue
                removed = self._run_redis_operation(
                    "requeue_stale_jobs_hdel",
                    lambda job_id=job_id: self._redis.hdel(self._inflight_key, job_id),
                )
                if not removed:
                    continue
                self._run_redis_operation(
                    "requeue_stale_jobs_lpush",
                    lambda payload=entry["payload"]: self._redis.lpush(
                        self._queue_name, payload
                    ),
                )
                requeued += 1
                logger.info(
                    "[Queue:redis] requeued stale job_id=%s age=%.0fs", job_id, age_seconds
                )
            except Exception as exc:
                logger.warning(
                    "[Queue:redis] stale check failed job_id=%s: %s", job_id, exc
                )
                self._metrics.on_failure(self.backend_name, "stale_recovery")
        self._metrics.on_snapshot(self.backend_name, self.get_metrics())
        return requeued

    # ── Metrics ───────────────────────────────────────────────────────────

    def get_metrics(self) -> dict:
        queue_depth = self._redis.llen(self._queue_name)
        delayed_jobs = self._redis.zcard(self._delayed_key)
        return {
            "queue_depth": queue_depth,
            "in_flight_count": self._redis.hlen(self._inflight_key),
            "failed_jobs": self.get_dlq_depth(),
            "delayed_jobs": delayed_jobs,
            "dlq_depth": self.get_dlq_depth(),
            "max_queue_size": self._max_queue_size,
            "total_pending_jobs": queue_depth + delayed_jobs,
            "saturation_threshold": _saturation_threshold(),
        }

    def get_dlq_depth(self) -> int:
        return int(self._redis.llen(self._dlq_key))

    def peek_dead_letters(self, n: int) -> list[dict]:
        entries = self._redis.lrange(self._dlq_key, 0, max(0, n) - 1)
        return [json.loads(entry) for entry in entries]

    def remove_dead_letter(self, job_id: str) -> bool:
        entries = self._run_redis_operation(
            "remove_dead_letter_lrange",
            lambda: self._redis.lrange(self._dlq_key, 0, -1),
        )
        target_raw = None
        for entry_raw in entries:
            try:
                if json.loads(entry_raw).get("job_id") == job_id:
                    target_raw = entry_raw
                    break
            except Exception:
                continue
        if target_raw is None:
            return False
        removed = int(
            self._run_redis_operation(
                "remove_dead_letter_lrem",
                lambda: self._redis.lrem(self._dlq_key, 1, target_raw),
            )
        )
        if removed:
            self._metrics.on_snapshot(self.backend_name, self.get_metrics())
        return removed > 0

    def drain_dead_letters(self) -> int:
        count = self.get_dlq_depth()
        if count <= 0:
            return 0
        self._run_redis_operation(
            "drain_dead_letters_del",
            lambda: self._redis.delete(self._dlq_key),
        )
        self._metrics.on_snapshot(self.backend_name, self.get_metrics())
        return count


# ---------------------------------------------------------------------------
# In-memory backend (tests / single-process dev)
# ---------------------------------------------------------------------------

class InMemoryQueueBackend(DistributedQueueBackend):
    """Thread-safe in-process FIFO queue backed by ``queue.Queue``.

    Implements all reliability features of ``RedisQueueBackend``:
    in-flight tracking, Dead Letter Queue, delayed enqueue (via
    ``threading.Timer``), and full ``get_metrics()``.

    Suitable for unit tests and single-process development without Redis.
    **NOT usable across OS processes** — items live in this process's heap only.
    """

    def __init__(
        self,
        max_queue_size: int | None = None,
        *,
        metrics: Optional[QueueMetrics] = None,
        degraded: bool = False,
        fallback_reason: str | None = None,
    ) -> None:
        self._max_queue_size = max_queue_size or _queue_capacity_limit()
        self._metrics = metrics or QueueMetrics()
        self._degraded = degraded
        self._fallback_reason = fallback_reason
        self._q: queue.Queue[QueueJobPayload] = queue.Queue(maxsize=self._max_queue_size)
        self._inflight: dict[str, tuple[QueueJobPayload, datetime]] = {}
        self._inflight_lock = threading.Lock()
        self._dlq: list[dict] = []
        self._dlq_lock = threading.Lock()
        self._timers: list[threading.Timer] = []
        self._timers_lock = threading.Lock()
        self._delayed_count = 0

    @property
    def degraded(self) -> bool:
        return self._degraded

    @property
    def redis_available(self) -> bool:
        return False

    @property
    def fallback_reason(self) -> str | None:
        return self._fallback_reason

    def _pending_depth(self) -> int:
        with self._timers_lock:
            delayed = self._delayed_count
        return self._q.qsize() + delayed

    def _reject_if_full(self) -> None:
        if self._pending_depth() >= self._max_queue_size:
            self._metrics.on_enqueue(self.backend_name, "rejected")
            raise QueueSaturatedError(
                f"Queue is saturated (capacity={self._max_queue_size}). Retry later."
            )

    def enqueue(self, payload: QueueJobPayload) -> None:
        self._reject_if_full()
        self._q.put_nowait(payload)
        self._metrics.on_enqueue(self.backend_name, "accepted")
        logger.debug(
            "[Queue:mem] enqueued job_id=%s task=%s", payload.job_id, payload.task_name
        )
        self._metrics.on_snapshot(self.backend_name, self.get_metrics())

    def dequeue(self, timeout: int = 5) -> Optional[QueueJobPayload]:
        try:
            job = self._q.get(timeout=timeout)
        except queue.Empty:
            return None
        with self._inflight_lock:
            self._inflight[job.job_id] = (job, datetime.now(timezone.utc))
        self._metrics.on_dequeue(self.backend_name)
        self._metrics.on_snapshot(self.backend_name, self.get_metrics())
        return job

    def ack(self, job_id: str) -> None:
        with self._inflight_lock:
            self._inflight.pop(job_id, None)
        logger.debug("[Queue:mem] ack job_id=%s", job_id)
        self._metrics.on_snapshot(self.backend_name, self.get_metrics())

    def fail(self, job_id: str, error: str = "") -> None:
        with self._inflight_lock:
            entry = self._inflight.pop(job_id, None)
        with self._dlq_lock:
            self._dlq.append({
                "job_id": job_id,
                "payload_raw": entry[0].to_json() if entry else "",
                "error": error,
                "failed_at": datetime.now(timezone.utc).isoformat(),
            })
        self._metrics.on_failure(self.backend_name, "job")
        logger.warning("[Queue:mem] fail→DLQ job_id=%s error=%s", job_id, error)
        self._metrics.on_snapshot(self.backend_name, self.get_metrics())

    def enqueue_delayed(self, payload: QueueJobPayload, delay_seconds: float) -> None:
        """Schedule enqueue after *delay_seconds* using a daemon Timer."""
        self._reject_if_full()
        with self._timers_lock:
            self._delayed_count += 1

        def _fire() -> None:
            try:
                self._q.put_nowait(payload)
            finally:
                with self._timers_lock:
                    self._delayed_count = max(0, self._delayed_count - 1)
                self._metrics.on_snapshot(self.backend_name, self.get_metrics())
            logger.debug("[Queue:mem] delayed enqueue fired job_id=%s", payload.job_id)

        t = threading.Timer(delay_seconds, _fire)
        t.daemon = True
        t.start()
        with self._timers_lock:
            self._timers = [x for x in self._timers if x.is_alive()]
            self._timers.append(t)
        logger.debug(
            "[Queue:mem] delayed enqueue job_id=%s delay=%.1fs", payload.job_id, delay_seconds
        )
        self._metrics.on_enqueue(self.backend_name, "accepted")
        self._metrics.on_snapshot(self.backend_name, self.get_metrics())

    def requeue_stale_jobs(self, timeout_seconds: int = 300) -> int:
        now = datetime.now(timezone.utc)
        to_requeue: list[tuple[str, QueueJobPayload]] = []
        with self._inflight_lock:
            for job_id, (job, dequeued_at) in list(self._inflight.items()):
                age = (now - dequeued_at).total_seconds()
                if age > timeout_seconds:
                    to_requeue.append((job_id, job))
            for job_id, _ in to_requeue:
                del self._inflight[job_id]
        for _, job in to_requeue:
            self._q.put(job)
            logger.info("[Queue:mem] requeued stale job_id=%s", job.job_id)
        self._metrics.on_snapshot(self.backend_name, self.get_metrics())
        return len(to_requeue)

    def get_metrics(self) -> dict:
        with self._inflight_lock:
            inflight = len(self._inflight)
        with self._dlq_lock:
            dlq = len(self._dlq)
        return {
            "queue_depth": self._q.qsize(),
            "in_flight_count": inflight,
            "failed_jobs": dlq,
            "delayed_jobs": self._delayed_count,
            "dlq_depth": dlq,
            "max_queue_size": self._max_queue_size,
            "total_pending_jobs": self._q.qsize() + self._delayed_count,
            "saturation_threshold": _saturation_threshold(),
        }

    def get_dlq_depth(self) -> int:
        with self._dlq_lock:
            return len(self._dlq)

    # ── Test helpers ──────────────────────────────────────────────────────

    def qsize(self) -> int:
        """Number of items currently waiting (for test assertions)."""
        return self._q.qsize()

    def get_dead_letters(self) -> list[dict]:
        """Return a copy of the DLQ (for test assertions)."""
        with self._dlq_lock:
            return list(self._dlq)

    def get_inflight_ids(self) -> list[str]:
        """Return current in-flight job IDs (for test assertions)."""
        with self._inflight_lock:
            return list(self._inflight.keys())

    def remove_dead_letter(self, job_id: str) -> bool:
        removed = False
        with self._dlq_lock:
            for idx, entry in enumerate(self._dlq):
                if entry.get("job_id") == job_id:
                    del self._dlq[idx]
                    removed = True
                    break
        if removed:
            self._metrics.on_snapshot(self.backend_name, self.get_metrics())
        return removed

    def drain_dead_letters(self) -> int:
        with self._dlq_lock:
            count = len(self._dlq)
            self._dlq.clear()
        if count:
            self._metrics.on_snapshot(self.backend_name, self.get_metrics())
        return count
