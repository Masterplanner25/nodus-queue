"""QueueJobPayload — serialisable representation of one distributed async job."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone


@dataclass
class QueueJobPayload:
    """Serialisable representation of one distributed async job.

    The payload is intentionally lightweight.  Workers should re-fetch the
    full job record from their own data store using ``job_id`` rather than
    embedding large blobs in the queue entry.
    """

    job_id: str
    """Primary key — used to look up the full record from the data store."""

    task_name: str
    """Registered handler key (e.g. ``"agent.create_run"``)."""

    idempotency_key: str = ""
    """Deduplication key.  Defaults to ``job_id`` when not explicitly set.
    Workers may check this to guard against double-execution after a
    visibility-timeout re-enqueue.
    """

    context: dict = field(default_factory=dict)
    """Execution context carried across the worker boundary:
    ``trace_id``, ``eu_id``, ``user_id``, ``capabilities``.
    """

    retry_metadata: dict = field(default_factory=dict)
    """``attempt_count``, ``max_attempts``, ``is_retry`` — carried across
    re-enqueues so the worker can restore the correct retry state.
    """

    enqueued_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    """Wall-clock timestamp at enqueue time (UTC ISO 8601)."""

    def __post_init__(self) -> None:
        if not self.idempotency_key:
            self.idempotency_key = self.job_id

    def to_json(self) -> str:
        """Serialise to a compact JSON string (wire format)."""
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, raw: str) -> "QueueJobPayload":
        """Deserialise from a JSON string.  Unknown fields are silently dropped."""
        data = json.loads(raw)
        known = set(cls.__dataclass_fields__)
        return cls(**{k: v for k, v in data.items() if k in known})
