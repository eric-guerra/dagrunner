CREATE TABLE dag_runs (
    id           BIGSERIAL PRIMARY KEY,
    dag_id       VARCHAR(64) NOT NULL,
    logical_date DATE        NOT NULL,
    state        VARCHAR(16) NOT NULL,
    triggered_by VARCHAR(32) NOT NULL,
    created_at   TIMESTAMP   NOT NULL,
    ended_at     TIMESTAMP,
    CONSTRAINT uq_dag_run UNIQUE (dag_id, logical_date)
);
CREATE INDEX ix_runs_state ON dag_runs (state);

CREATE TABLE task_instances (
    run_id          BIGINT      NOT NULL REFERENCES dag_runs (id) ON DELETE CASCADE,
    task_id         VARCHAR(64) NOT NULL,
    state           VARCHAR(16) NOT NULL,
    attempts        INT         NOT NULL,
    next_attempt_at TIMESTAMP,
    started_at      TIMESTAMP,
    ended_at        TIMESTAMP,
    last_error      VARCHAR(1000),
    PRIMARY KEY (run_id, task_id)
);

CREATE TABLE task_logs (
    id      BIGSERIAL PRIMARY KEY,
    run_id  BIGINT        NOT NULL REFERENCES dag_runs (id) ON DELETE CASCADE,
    task_id VARCHAR(64)   NOT NULL,
    attempt INT           NOT NULL,
    line    VARCHAR(2000) NOT NULL,
    at      TIMESTAMP     NOT NULL
);
CREATE INDEX ix_logs_task ON task_logs (run_id, task_id);
