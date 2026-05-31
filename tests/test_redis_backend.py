"""Redis backend tests using fakeredis.

Requires: pip install fakeredis[lua]
Skips automatically when fakeredis is not installed.
"""
from __future__ import annotations

import json
import time
from unittest.mock import MagicMock

import pytest

from nodus_queue import QueueJobPayload, QueueMetrics, QueueSaturatedError

fakeredis = pytest.importorskip("fakeredis", reason="fakeredis not installed")

try:
    import fakeredis.aioredis  # noqa: F401
except ImportError:
    pass


def _job(job_id: str = "j1", task: str = "t") -> QueueJobPayload:
    return QueueJobPayload(job_id=job_id, task_name=task)


def _make_backend(max_queue_size: int | None = None, metrics=None):
    """Build a RedisQueueBackend wired to a fakeredis instance."""
    from nodus_queue import RedisQueueBackend

    fake_redis = fakeredis.FakeRedis(decode_responses=True)

    backend = RedisQueueBackend.__new__(RedisQueueBackend)
    backend._redis = fake_redis
    backend._queue_name = "test:jobs"
    backend._max_queue_size = max_queue_size or 100
    backend._inflight_key = "test:jobs:inflight"
    backend._delayed_key = "test:jobs:delayed"
    backend._dlq_key = "test:jobs:dead"

    # Register Lua scripts against fakeredis
    backend._process_delayed = fake_redis.register_script(
        RedisQueueBackend._PROCESS_DELAYED_LUA
    )
    backend._enqueue_with_capacity = fake_redis.register_script(
        RedisQueueBackend._ENQUEUE_WITH_CAPACITY_LUA
    )
    backend._enqueue_delayed_with_capacity = fake_redis.register_script(
        RedisQueueBackend._ENQUEUE_DELAYED_WITH_CAPACITY_LUA
    )
    import redis as _r
    backend._redis_exceptions = (
        _r.ConnectionError, _r.TimeoutError, _r.BusyLoadingError
    )
    backend._failure_count = 0
    backend._open_until = 0.0
    backend._circuit_breaker_threshold = 5
    backend._circuit_breaker_open_seconds = 30.0
    backend._metrics = metrics or QueueMetrics()
    return backend


# ── Basic lifecycle ───────────────────────────────────────────────────────────

def test_enqueue_and_dequeue():
    q = _make_backend()
    q.enqueue(_job("j1"))
    result = q.dequeue(timeout=0)
    assert result is not None
    assert result.job_id == "j1"


def test_dequeue_returns_none_when_empty():
    q = _make_backend()
    result = q.dequeue(timeout=0)
    assert result is None


def test_job_added_to_inflight_on_dequeue():
    q = _make_backend()
    q.enqueue(_job("j1"))
    q.dequeue(timeout=0)
    assert q._redis.hget(q._inflight_key, "j1") is not None


def test_ack_removes_from_inflight():
    q = _make_backend()
    q.enqueue(_job("j1"))
    q.dequeue(timeout=0)
    q.ack("j1")
    assert q._redis.hget(q._inflight_key, "j1") is None


def test_fail_moves_to_dlq():
    q = _make_backend()
    q.enqueue(_job("j1"))
    q.dequeue(timeout=0)
    q.fail("j1", error="boom")
    assert q.get_dlq_depth() == 1
    entries = q.peek_dead_letters(1)
    assert entries[0]["job_id"] == "j1"
    assert entries[0]["error"] == "boom"


# ── Saturation ────────────────────────────────────────────────────────────────

def test_saturation_raises():
    q = _make_backend(max_queue_size=2)
    q.enqueue(_job("j1"))
    q.enqueue(_job("j2"))
    with pytest.raises(QueueSaturatedError):
        q.enqueue(_job("j3"))


# ── Delayed enqueue ───────────────────────────────────────────────────────────

def test_enqueue_delayed_and_process():
    q = _make_backend()
    # Set execute_at to the past so it's immediately ready
    job = _job("j-delayed")
    raw = job.to_json()
    import time as _time
    past_ts = _time.time() - 10
    q._redis.zadd(q._delayed_key, {raw: past_ts})

    promoted = q.process_delayed_jobs()
    assert promoted == 1
    result = q.dequeue(timeout=0)
    assert result is not None
    assert result.job_id == "j-delayed"


# ── Stale job recovery ────────────────────────────────────────────────────────

def test_requeue_stale_jobs():
    q = _make_backend()
    q.enqueue(_job("j1"))
    q.dequeue(timeout=0)
    # Manually set dequeued_at to the past
    entry = json.loads(q._redis.hget(q._inflight_key, "j1"))
    entry["dequeued_at"] = "2020-01-01T00:00:00+00:00"
    q._redis.hset(q._inflight_key, "j1", json.dumps(entry))

    requeued = q.requeue_stale_jobs(timeout_seconds=1)
    assert requeued == 1
    assert q._redis.hget(q._inflight_key, "j1") is None
    result = q.dequeue(timeout=0)
    assert result is not None
    assert result.job_id == "j1"


# ── DLQ management ────────────────────────────────────────────────────────────

def test_remove_dead_letter():
    q = _make_backend()
    q.enqueue(_job("j1"))
    q.dequeue(timeout=0)
    q.fail("j1")
    assert q.remove_dead_letter("j1") is True
    assert q.get_dlq_depth() == 0


def test_drain_dead_letters():
    q = _make_backend()
    for i in range(3):
        q.enqueue(_job(f"j{i}"))
        q.dequeue(timeout=0)
        q.fail(f"j{i}")
    assert q.drain_dead_letters() == 3
    assert q.get_dlq_depth() == 0


# ── Metrics ───────────────────────────────────────────────────────────────────

def test_metrics_on_enqueue():
    m = MagicMock(spec=QueueMetrics)
    q = _make_backend(metrics=m)
    q.enqueue(_job("j1"))
    m.on_enqueue.assert_called_once_with("redis", "accepted")


def test_metrics_on_dequeue():
    m = MagicMock(spec=QueueMetrics)
    q = _make_backend(metrics=m)
    q.enqueue(_job("j1"))
    m.reset_mock()
    q.dequeue(timeout=0)
    m.on_dequeue.assert_called_once_with("redis")


def test_metrics_on_failure():
    m = MagicMock(spec=QueueMetrics)
    q = _make_backend(metrics=m)
    q.enqueue(_job("j1"))
    q.dequeue(timeout=0)
    m.reset_mock()
    q.fail("j1", "boom")
    m.on_failure.assert_called_once_with("redis", "job")


def test_get_metrics_shape():
    q = _make_backend()
    q.enqueue(_job("j1"))
    q.dequeue(timeout=0)
    m = q.get_metrics()
    assert m["queue_depth"] == 0
    assert m["in_flight_count"] == 1
    assert m["delayed_jobs"] == 0
