package com.ericguerra.dag.engine;

import com.ericguerra.dag.model.TaskState;

import java.time.Instant;

public class TaskInstance {
    private final String taskId;
    private TaskState state = TaskState.PENDING;
    private int attempts;
    private Instant nextAttemptAt;
    private Instant startedAt;
    private Instant endedAt;
    private String lastError;

    public TaskInstance(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskId() { return taskId; }
    public TaskState getState() { return state; }
    public void setState(TaskState state) { this.state = state; }
    public int getAttempts() { return attempts; }
    public void setAttempts(int attempts) { this.attempts = attempts; }
    public Instant getNextAttemptAt() { return nextAttemptAt; }
    public void setNextAttemptAt(Instant t) { this.nextAttemptAt = t; }
    public Instant getStartedAt() { return startedAt; }
    public void setStartedAt(Instant t) { this.startedAt = t; }
    public Instant getEndedAt() { return endedAt; }
    public void setEndedAt(Instant t) { this.endedAt = t; }
    public String getLastError() { return lastError; }
    public void setLastError(String e) { this.lastError = e; }
}
