"""Optional metrics integration hook.

Subclass ``QueueMetrics`` and pass an instance to backend constructors or
``get_queue()`` to wire up Prometheus (or any other metrics system) without
adding a hard dependency on ``prometheus-client``.

Example — Prometheus integration::

    from prometheus_client import CollectorRegistry, Counter, Gauge
    from nodus_queue import QueueMetrics

    REGISTRY = CollectorRegistry()
    enqueue_total = Counter("queue_enqueue_total", "...", ["backend", "outcome"], registry=REGISTRY)
    # ... define other metrics ...

    class PrometheusQueueMetrics(QueueMetrics):
        def on_enqueue(self, backend: str, outcome: str) -> None:
            enqueue_total.labels(backend=backend, outcome=outcome).inc()
        # override other methods as needed
"""
from __future__ import annotations


class QueueMetrics:
    """No-op base class for queue metrics hooks.

    All methods are no-ops by default.  Override only the ones you need.
    """

    def on_enqueue(self, backend: str, outcome: str) -> None:
        """Called after every enqueue attempt.  ``outcome``: ``"accepted"`` or ``"rejected"``."""

    def on_dequeue(self, backend: str) -> None:
        """Called after a successful dequeue."""

    def on_failure(self, backend: str, stage: str) -> None:
        """Called when a job is moved to the DLQ or a stale-recovery fails.
        ``stage``: ``"job"`` | ``"stale_recovery"``.
        """

    def on_fallback(self) -> None:
        """Called when the Redis backend is unavailable and the queue falls
        back to in-memory mode.
        """

    def on_backend_mode_changed(self, is_redis: bool) -> None:
        """Called when the active backend type changes (redis ↔ memory)."""

    def on_snapshot(self, backend: str, snapshot: dict) -> None:
        """Called after any operation that changes queue depth, with the
        current metrics snapshot.  Use to update Gauges.
        """
