from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from nodus_queue import InMemoryQueueBackend, QueueJobPayload, QueueMetrics, QueueSaturatedError


def _job(job_id: str = "j1", task: str = "t") -> QueueJobPayload:
    return QueueJobPayload(job_id=job_id, task_name=task)


# ── Basic lifecycle ───────────────────────────────────────────────────────────

def test_enqueue_and_dequeue():
    q = InMemoryQueueBackend()
    job = _job()
    q.enqueue(job)
    result = q.dequeue(timeout=1)
    assert result is not None
    assert result.job_id == "j1"


def test_dequeue_returns_none_when_empty():
    q = InMemoryQueueBackend()
    result = q.dequeue(timeout=0)
    assert result is None


def test_ack_removes_from_inflight():
    q = InMemoryQueueBackend()
    q.enqueue(_job("j1"))
    q.dequeue(timeout=1)
    assert "j1" in q.get_inflight_ids()
    q.ack("j1")
    assert "j1" not in q.get_inflight_ids()


def test_fail_moves_to_dlq():
    q = InMemoryQueueBackend()
    q.enqueue(_job("j1"))
    q.dequeue(timeout=1)
    q.fail("j1", error="something broke")
    dlq = q.get_dead_letters()
    assert len(dlq) == 1
    assert dlq[0]["job_id"] == "j1"
    assert dlq[0]["error"] == "something broke"
    assert "j1" not in q.get_inflight_ids()


def test_fail_unknown_job_still_adds_to_dlq():
    q = InMemoryQueueBackend()
    q.fail("unknown-job", error="no record")
    dlq = q.get_dead_letters()
    assert len(dlq) == 1
    assert dlq[0]["payload_raw"] == ""


# ── Saturation ────────────────────────────────────────────────────────────────

def test_saturation_raises_on_enqueue():
    q = InMemoryQueueBackend(max_queue_size=2)
    q.enqueue(_job("j1"))
    q.enqueue(_job("j2"))
    with pytest.raises(QueueSaturatedError):
        q.enqueue(_job("j3"))


def test_saturation_error_carries_status_code():
    q = InMemoryQueueBackend(max_queue_size=1)
    q.enqueue(_job("j1"))
    with pytest.raises(QueueSaturatedError) as exc_info:
        q.enqueue(_job("j2"))
    assert exc_info.value.status_code == 503


# ── Stale job recovery ────────────────────────────────────────────────────────

def test_requeue_stale_jobs():
    q = InMemoryQueueBackend()
    q.enqueue(_job("j1"))
    q.dequeue(timeout=1)
    # Force stale immediately with timeout=0
    requeued = q.requeue_stale_jobs(timeout_seconds=-1)
    assert requeued == 1
    assert "j1" not in q.get_inflight_ids()
    # Job should be back in the queue
    result = q.dequeue(timeout=1)
    assert result is not None
    assert result.job_id == "j1"


def test_recent_inflight_not_requeued():
    q = InMemoryQueueBackend()
    q.enqueue(_job("j1"))
    q.dequeue(timeout=1)
    requeued = q.requeue_stale_jobs(timeout_seconds=9999)
    assert requeued == 0


# ── Delayed enqueue ───────────────────────────────────────────────────────────

def test_enqueue_delayed_fires_after_delay():
    q = InMemoryQueueBackend()
    q.enqueue_delayed(_job("j-delayed"), delay_seconds=0.05)
    assert q.qsize() == 0  # not yet in main queue
    time.sleep(0.15)
    assert q.qsize() == 1
    result = q.dequeue(timeout=1)
    assert result is not None
    assert result.job_id == "j-delayed"


# ── DLQ management ────────────────────────────────────────────────────────────

def test_get_dlq_depth():
    q = InMemoryQueueBackend()
    assert q.get_dlq_depth() == 0
    q.enqueue(_job("j1"))
    q.dequeue(timeout=1)
    q.fail("j1")
    assert q.get_dlq_depth() == 1


def test_remove_dead_letter():
    q = InMemoryQueueBackend()
    q.enqueue(_job("j1"))
    q.dequeue(timeout=1)
    q.fail("j1")
    assert q.remove_dead_letter("j1") is True
    assert q.get_dlq_depth() == 0


def test_remove_dead_letter_not_found():
    q = InMemoryQueueBackend()
    assert q.remove_dead_letter("nonexistent") is False


def test_drain_dead_letters():
    q = InMemoryQueueBackend()
    for i in range(3):
        q.enqueue(_job(f"j{i}"))
        q.dequeue(timeout=1)
        q.fail(f"j{i}")
    assert q.drain_dead_letters() == 3
    assert q.get_dlq_depth() == 0


# ── Metrics callback ──────────────────────────────────────────────────────────

def test_metrics_on_enqueue_called():
    m = MagicMock(spec=QueueMetrics)
    q = InMemoryQueueBackend(metrics=m)
    q.enqueue(_job("j1"))
    m.on_enqueue.assert_called_once_with("inmemory", "accepted")


def test_metrics_on_dequeue_called():
    m = MagicMock(spec=QueueMetrics)
    q = InMemoryQueueBackend(metrics=m)
    q.enqueue(_job("j1"))
    m.reset_mock()
    q.dequeue(timeout=1)
    m.on_dequeue.assert_called_once_with("inmemory")


def test_metrics_on_failure_called():
    m = MagicMock(spec=QueueMetrics)
    q = InMemoryQueueBackend(metrics=m)
    q.enqueue(_job("j1"))
    q.dequeue(timeout=1)
    m.reset_mock()
    q.fail("j1", "boom")
    m.on_failure.assert_called_once_with("inmemory", "job")


def test_metrics_on_enqueue_rejected_when_full():
    m = MagicMock(spec=QueueMetrics)
    q = InMemoryQueueBackend(max_queue_size=1, metrics=m)
    q.enqueue(_job("j1"))
    m.reset_mock()
    with pytest.raises(QueueSaturatedError):
        q.enqueue(_job("j2"))
    m.on_enqueue.assert_called_once_with("inmemory", "rejected")


# ── Metrics snapshot ──────────────────────────────────────────────────────────

def test_get_metrics_shape():
    q = InMemoryQueueBackend(max_queue_size=50)
    q.enqueue(_job("j1"))
    q.dequeue(timeout=1)
    m = q.get_metrics()
    assert m["queue_depth"] == 0
    assert m["in_flight_count"] == 1
    assert m["max_queue_size"] == 50


# ── Degraded mode ─────────────────────────────────────────────────────────────

def test_degraded_flag():
    q = InMemoryQueueBackend(degraded=True, fallback_reason="redis down")
    assert q.degraded is True
    assert q.fallback_reason == "redis down"
    assert q.redis_available is False


def test_health_snapshot_includes_backend_name():
    q = InMemoryQueueBackend()
    snap = q.health_snapshot()
    assert snap["backend"] == "memory"
    assert snap["degraded"] is False
