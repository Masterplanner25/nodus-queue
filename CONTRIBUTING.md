# Contributing to nodus-queue

## Setup

```bash
git clone https://github.com/Masterplanner25/nodus-queue.git
cd nodus-queue
pip install -e ".[dev]"
```

The `dev` extra includes `fakeredis` so Redis backend tests can run without
a live Redis server.

## Running tests

```bash
# Without live Redis (uses fakeredis)
pytest tests/ -q

# Redis backend tests require a live Redis server
pytest tests/test_redis_backend.py -q
```

## Code style

- Python 3.11+
- `InMemoryQueueBackend` must remain a drop-in replacement for
  `RedisQueueBackend` — same `DistributedQueueBackend` protocol
- `QueueMetrics` is a noop base class — override hooks to add Prometheus
- Call `reset_queue()` between tests to clear the singleton
- `tmp_demo/` is excluded from git — do not commit demo artefacts

## Submitting changes

1. Fork the repo and create a branch from `main`
2. Add tests for any new behaviour
3. Ensure `pytest tests/ -q --ignore=tests/test_redis_backend.py` passes
4. Open a pull request with a description of what changes and why
