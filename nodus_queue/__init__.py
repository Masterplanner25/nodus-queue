"""nodus-queue — distributed job queue with DLQ, delayed jobs, and in-flight tracking.

Backends:
    RedisQueueBackend       — LPUSH/BRPOP; single-consumer atomic; Lua-script capacity guard
    InMemoryQueueBackend    — thread-safe; Timer-based delayed enqueue; for tests and dev

Payload:
    QueueJobPayload         — serialisable job envelope with idempotency key

Metrics hook:
    QueueMetrics            — optional noop base class; subclass to wire Prometheus

Errors:
    QueueSaturatedError     — raised when the queue rejects work at capacity

Factory:
    get_queue()             — return the singleton backend (Redis or in-memory fallback)
    reset_queue()           — reset singleton for test isolation
    validate_queue_backend() — fail fast if backend is unavailable
    get_queue_health_snapshot() — health dict for monitoring
    attempt_queue_backend_reconnect() — try to restore Redis after degraded fallback
"""
from .backends import (
    QUEUE_NAME_DEFAULT,
    DistributedQueueBackend,
    InMemoryQueueBackend,
    QueueSaturatedError,
    RedisQueueBackend,
)
from .metrics import QueueMetrics
from .payload import QueueJobPayload
from .queue import (
    attempt_queue_backend_reconnect,
    get_queue,
    get_queue_health_snapshot,
    reset_queue,
    validate_queue_backend,
)

__all__ = [
    # Backends
    "DistributedQueueBackend",
    "InMemoryQueueBackend",
    "RedisQueueBackend",
    "QUEUE_NAME_DEFAULT",
    # Payload
    "QueueJobPayload",
    # Metrics
    "QueueMetrics",
    # Errors
    "QueueSaturatedError",
    # Factory
    "get_queue",
    "reset_queue",
    "validate_queue_backend",
    "get_queue_health_snapshot",
    "attempt_queue_backend_reconnect",
]
