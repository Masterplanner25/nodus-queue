from __future__ import annotations

import json

import pytest

from nodus_queue import QueueJobPayload


def _make(**kwargs) -> QueueJobPayload:
    return QueueJobPayload(
        job_id=kwargs.get("job_id", "job-1"),
        task_name=kwargs.get("task_name", "my.task"),
        **{k: v for k, v in kwargs.items() if k not in ("job_id", "task_name")},
    )


def test_default_idempotency_key_equals_job_id():
    p = _make(job_id="abc-123")
    assert p.idempotency_key == "abc-123"


def test_explicit_idempotency_key_preserved():
    p = _make(job_id="j1", idempotency_key="idem-xyz")
    assert p.idempotency_key == "idem-xyz"


def test_enqueued_at_is_iso_utc():
    p = _make()
    from datetime import datetime
    dt = datetime.fromisoformat(p.enqueued_at)
    assert dt.tzinfo is not None


def test_to_json_round_trip():
    p = _make(context={"trace_id": "t1"}, retry_metadata={"attempt_count": 2})
    raw = p.to_json()
    data = json.loads(raw)
    assert data["job_id"] == "job-1"
    assert data["context"]["trace_id"] == "t1"
    assert data["retry_metadata"]["attempt_count"] == 2


def test_from_json_round_trip():
    original = _make(job_id="j99", task_name="foo.bar", context={"user_id": "u1"})
    restored = QueueJobPayload.from_json(original.to_json())
    assert restored.job_id == original.job_id
    assert restored.task_name == original.task_name
    assert restored.context == original.context
    assert restored.idempotency_key == original.idempotency_key


def test_from_json_ignores_unknown_fields():
    data = {"job_id": "j1", "task_name": "t", "unknown_field": "ignored"}
    p = QueueJobPayload.from_json(json.dumps(data))
    assert p.job_id == "j1"
    assert not hasattr(p, "unknown_field")


def test_from_json_idempotency_key_defaults_to_job_id_when_missing():
    data = {"job_id": "j1", "task_name": "t"}  # no idempotency_key
    p = QueueJobPayload.from_json(json.dumps(data))
    assert p.idempotency_key == "j1"
