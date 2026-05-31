from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

from nodus_queue import (
    InMemoryQueueBackend,
    QueueMetrics,
    get_queue,
    get_queue_health_snapshot,
    reset_queue,
)


@pytest.fixture(autouse=True)
def _reset():
    """Ensure the singleton is cleared before and after every test."""
    reset_queue()
    yield
    reset_queue()


# ── Test-mode detection ───────────────────────────────────────────────────────

def test_testing_env_gives_memory_backend():
    with patch.dict(os.environ, {"TESTING": "true", "REDIS_URL": ""}):
        backend = get_queue()
    assert isinstance(backend, InMemoryQueueBackend)


def test_test_mode_env_gives_memory_backend():
    with patch.dict(os.environ, {"TEST_MODE": "true", "REDIS_URL": ""}):
        backend = get_queue()
    assert isinstance(backend, InMemoryQueueBackend)


# ── No Redis configured ───────────────────────────────────────────────────────

def test_no_redis_url_gives_memory_backend():
    env = {"REDIS_URL": "", "TESTING": "false", "TEST_MODE": "false", "ENV": ""}
    with patch.dict(os.environ, env, clear=False):
        backend = get_queue()
    assert isinstance(backend, InMemoryQueueBackend)
    assert backend.degraded is False


# ── force_memory ──────────────────────────────────────────────────────────────

def test_force_memory_returns_fresh_instance():
    b1 = get_queue(force_memory=True)
    b2 = get_queue(force_memory=True)
    assert isinstance(b1, InMemoryQueueBackend)
    assert isinstance(b2, InMemoryQueueBackend)
    assert b1 is not b2  # not cached


def test_force_memory_does_not_cache():
    get_queue(force_memory=True)
    # Singleton should still be None — next call without force_memory picks env
    env = {"REDIS_URL": "", "TESTING": "false", "TEST_MODE": "false", "ENV": ""}
    with patch.dict(os.environ, env, clear=False):
        backend = get_queue()
    assert isinstance(backend, InMemoryQueueBackend)


# ── Singleton caching ─────────────────────────────────────────────────────────

def test_get_queue_returns_same_instance():
    env = {"REDIS_URL": "", "TESTING": "false", "TEST_MODE": "false", "ENV": ""}
    with patch.dict(os.environ, env, clear=False):
        b1 = get_queue()
        b2 = get_queue()
    assert b1 is b2


def test_reset_queue_clears_singleton():
    env = {"REDIS_URL": "", "TESTING": "false", "TEST_MODE": "false", "ENV": ""}
    with patch.dict(os.environ, env, clear=False):
        b1 = get_queue()
    reset_queue()
    with patch.dict(os.environ, env, clear=False):
        b2 = get_queue()
    assert b1 is not b2


# ── Production guard ──────────────────────────────────────────────────────────

def test_production_without_redis_raises():
    env = {"REDIS_URL": "", "ENV": "production", "TESTING": "false", "TEST_MODE": "false"}
    with patch.dict(os.environ, env, clear=False):
        with pytest.raises(RuntimeError, match="REDIS_URL"):
            get_queue()


def test_production_with_distributed_mode_and_no_redis_raises():
    env = {
        "REDIS_URL": "",
        "ENV": "development",
        "EXECUTION_MODE": "distributed",
        "TESTING": "false",
        "TEST_MODE": "false",
    }
    with patch.dict(os.environ, env, clear=False):
        with pytest.raises(RuntimeError, match="EXECUTION_MODE=distributed"):
            get_queue()


# ── Metrics hook ──────────────────────────────────────────────────────────────

def test_metrics_on_backend_mode_changed_called():
    m = MagicMock(spec=QueueMetrics)
    env = {"REDIS_URL": "", "TESTING": "false", "TEST_MODE": "false", "ENV": ""}
    with patch.dict(os.environ, env, clear=False):
        get_queue(metrics=m)
    m.on_backend_mode_changed.assert_called_once_with(False)


# ── Health snapshot ───────────────────────────────────────────────────────────

def test_health_snapshot_shape():
    env = {"REDIS_URL": "", "TESTING": "false", "TEST_MODE": "false", "ENV": ""}
    with patch.dict(os.environ, env, clear=False):
        snap = get_queue_health_snapshot()
    assert "backend" in snap
    assert "degraded" in snap
    assert "metrics" in snap
    assert snap["backend"] == "memory"
