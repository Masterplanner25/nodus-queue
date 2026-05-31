# nodus-queue

Distributed job queue with Dead Letter Queue, delayed jobs, in-flight tracking, and visibility-timeout recovery. Redis backend for multi-instance production; in-memory fallback for dev and tests. Zero hard dependencies beyond `tenacity`.

## Install

```bash
pip install nodus-queue              # core + in-memory backend
pip install "nodus-queue[redis]"     # + Redis backend
```

## Quickstart

```python
from nodus_queue import QueueJobPayload, get_queue, reset_queue

# In dev/test — in-memory backend (automatic when REDIS_URL is unset)
q = get_queue()

job = QueueJobPayload(job_id="run-123", task_name="agent.run")
q.enqueue(job)

# Worker side
job = q.dequeue(timeout=5)   # blocks up to 5 seconds
if job:
    try:
        # ... process job ...
        q.ack(job.job_id)    # remove from in-flight
    except Exception as e:
        q.fail(job.job_id, str(e))   # move to DLQ
```

## Redis backend

```bash
REDIS_URL=redis://localhost:6379/0
```

```python
from nodus_queue import get_queue

q = get_queue()   # picks up REDIS_URL automatically
```

## Delayed jobs

```python
# Schedule a job to run after 30 seconds
q.enqueue_delayed(job, delay_seconds=30)

# Promote ready jobs (call periodically in Redis mode)
count = q.process_delayed_jobs()
```

## Crash recovery

```python
# On worker startup — re-enqueue jobs stuck in-flight for > 5 minutes
q.requeue_stale_jobs(timeout_seconds=300)
```

## Dead Letter Queue

```python
depth = q.get_dlq_depth()
q.drain_dead_letters()               # clear all
q.remove_dead_letter("job-id-123")  # remove one
```

## Optional Prometheus metrics

```python
from prometheus_client import CollectorRegistry, Counter, Gauge
from nodus_queue import QueueMetrics, get_queue

REGISTRY = CollectorRegistry()
enq = Counter("queue_enqueue_total", "...", ["backend", "outcome"], registry=REGISTRY)

class MyMetrics(QueueMetrics):
    def on_enqueue(self, backend, outcome):
        enq.labels(backend=backend, outcome=outcome).inc()
    # override other hooks as needed

q = get_queue(metrics=MyMetrics())
```

## Backend change callback

```python
def on_change(event: str, payload: dict) -> None:
    print(f"Queue backend changed: {event} {payload}")

q = get_queue(on_backend_change=on_change)
```

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `REDIS_URL` | — | Redis connection URL |
| `NODUS_QUEUE_NAME` | `nodus:jobs` | Key prefix for all queue Redis keys |
| `NODUS_QUEUE_MAXSIZE` | `100` | Hard capacity limit |
| `NODUS_REQUIRE_REDIS` | `false` | Fail on startup if Redis is unavailable |
| `EXECUTION_MODE` | `thread` | `distributed` requires `REDIS_URL` |
| `ENV` | — | `production`/`prod` requires `REDIS_URL` |
| `TESTING` / `TEST_MODE` | — | Auto-select in-memory backend |

## Extracted from

`AINDY/core/distributed_queue.py` in the A.I.N.D.Y. runtime.
