# DagRunner

Portfolio reference implementation of a 2020 project, built to show a lightweight, Airflow-style workflow scheduler for nightly retail data pipelines (inventory, sales and replenishment loads).

**Stack:** Java 11, Spring Boot 2.7, Quartz (clustered JDBC job store), PostgreSQL, Flyway, Jackson YAML.

## Concepts
- **DAGs in YAML** (`dags/*.yml`). Each one has tasks, `dependsOn` edges, a cron `schedule`, and per-task `retries`, `retryDelay`, `timeout` and `triggerRule`. The loader validates ids, unknown dependencies and duplicates, and finds cycles with Kahn's algorithm (the error names the tasks in the cycle). A bad file is rejected without blocking the others.
- **Operators:**
  - `shell`: exit code ≠ 0 fails the task.
  - `http`: Java 11 HttpClient, sends an idempotency key.
  - `sql`: runs in one transaction; write it as delete-then-insert per `{{ ds }}`.
  - Config strings are templated with `{{ ds }}`, `{{ ds_nodash }}`, `{{ run_id }}` and `{{ attempt }}`.
- **Trigger rules:**
  - `all_success` (default): failures propagate downstream as `UPSTREAM_FAILED`.
  - `all_done`: for cleanup tasks.
  - `one_failed`: for alerting tasks; skipped when nothing failed.

## Engine
- `RunPlanner` makes the scheduling decisions as pure code: it resolves trigger rules in topological order and picks which tasks can start within `maxParallelism`.
- `RunExecutor` runs the threads:
  - It runs each task on a worker pool with a per-attempt timeout. Exactly one outcome is recorded per attempt, even if the timeout and completion race.
  - Retries use exponential backoff.
  - Every state change is persisted.
  - **Crash recovery:** on startup, runs that are still `RUNNING` are resumed, and tasks that were mid-flight get another attempt.
- **One run per DAG per logical date:** `UNIQUE (dag_id, logical_date)` makes double fires (two nodes, or a manual trigger racing cron) harmless. Quartz runs in clustered mode, so each cron fire happens on only one node. The logical date is taken from the *scheduled* fire time, so a late misfire still loads the right day.
- **Operations:** manual trigger, backfill over a date range with a concurrency limit, clearing a task (plus everything downstream) to re-run it inside the same run, and per-attempt task logs.

## API
| Method | Path | |
|---|---|---|
| GET | `/api/dags` | DAGs and their topological order |
| GET | `/api/dags/{id}/graph` | nodes and edges for the UI (see FlowGraph UI) |
| POST | `/api/dags/reload` | re-read the YAML; returns per-file errors |
| POST | `/api/dags/{id}/runs?ds=2020-03-01` | trigger a run (409 if it already exists) |
| POST | `/api/dags/{id}/backfill?from=&to=&concurrency=` | backfill a date range |
| GET | `/api/runs/{runId}` | run and task states |
| POST | `/api/runs/{runId}/tasks/{task}/clear?downstream=true` | re-run a task |
| GET | `/api/runs/{runId}/tasks/{task}/logs` | task log lines |

## Run and test
```bash
docker compose up -d postgres
mvn spring-boot:run
mvn test   # DAG model/loader, planner trigger rules, executor (parallelism, retries, timeouts, resume)
```
