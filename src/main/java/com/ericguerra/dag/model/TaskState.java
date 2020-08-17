package com.ericguerra.dag.model;

public enum TaskState {
    PENDING, QUEUED, RUNNING, UP_FOR_RETRY, SUCCESS, FAILED, SKIPPED, UPSTREAM_FAILED;

    public boolean isTerminal() {
        return this == SUCCESS || this == FAILED || this == SKIPPED || this == UPSTREAM_FAILED;
    }

    public boolean isFailure() {
        return this == FAILED || this == UPSTREAM_FAILED;
    }
}
