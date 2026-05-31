# Changelog

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning: [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

---

## [0.1.0] — 2026-05-31

Initial release — prepared, not yet published.

### Added

- **`QueueJobPayload`** — serialisable job envelope. Fields: `job_id`,
  `task_name`, `payload` (dict | None), `idempotency_key`, `enqueued_at`,
  `attempt_count`, `max_attempts`, `delay_seconds`. `to_json()` / `from_json()`.

- **`InMemoryQueueBackend`** — thread-safe in-process backend for dev and tests.
  Timer-based delayed enqueue. `enqueue(job)`, `dequeue(timeout?)`,
  `ack(job_id)`, `nack(job_id)`, `get_dlq()`, `drain_dlq()`, `len()`.

- **`RedisQueueBackend`** — LPUSH/BRPOP single-consumer backend. Lua-script
  capacity guard. Visibility-timeout in-flight tracking. Delayed jobs via
  Redis sorted set. DLQ on max-attempts exhaustion. `enqueue`, `dequeue`,
  `ack`, `nack`, `get_dlq`, `drain_dlq`. Requires `[redis]` extra.

- **`DistributedQueueBackend`** — protocol / ABC that both backends implement.

- **`QUEUE_NAME_DEFAULT`** — default queue name constant.

- **`QueueMetrics`** — noop base class; subclass and override hooks to wire
  Prometheus counters/gauges.

- **`QueueSaturatedError`** — raised when a backend rejects work at capacity.

- **`get_queue(redis_url?, queue_name?)`** — process-level singleton.
  Returns `RedisQueueBackend` when `REDIS_URL` is set, `InMemoryQueueBackend`
  otherwise.

- **`reset_queue()`** — clears singleton (for test isolation).

- **`validate_queue_backend()`** — raises if backend is unavailable.

- **`get_queue_health_snapshot()`** — returns health dict for monitoring.

- **`attempt_queue_backend_reconnect()`** — tries to restore Redis after
  degraded in-memory fallback.

- **53 tests** across 4 test files. Redis tests (`test_redis_backend.py`)
  require a live Redis server — skip with
  `--ignore=tests/test_redis_backend.py` in dev.

- **One required dependency:** `tenacity>=8.0.0` (retry logic in Redis
  reconnect). Optional `[redis]` extra adds `redis>=4.0.0`.

[0.1.0]: https://github.com/Masterplanner25/nodus-queue/releases/tag/v0.1.0
