package com.ericguerra.dag.engine;

import java.time.LocalDate;
import java.util.function.Consumer;

public class RunContext {
    private final long runId;
    private final String dagId;
    private final String taskId;
    private final LocalDate logicalDate;
    private final int attempt;
    private final Consumer<String> log;

    public RunContext(long runId, String dagId, String taskId, LocalDate logicalDate, int attempt, Consumer<String> log) {
        this.runId = runId;
        this.dagId = dagId;
        this.taskId = taskId;
        this.logicalDate = logicalDate;
        this.attempt = attempt;
        this.log = log;
    }

    /** Replace {{ ds }}, {{ run_id }} and {{ attempt }} placeholders in operator config. */
    public String render(String template) {
        return template
                .replace("{{ ds }}", logicalDate.toString())
                .replace("{{ ds_nodash }}", logicalDate.toString().replace("-", ""))
                .replace("{{ run_id }}", Long.toString(runId))
                .replace("{{ attempt }}", Integer.toString(attempt));
    }

    public void log(String line) { log.accept(line); }
    public long getRunId() { return runId; }
    public String getDagId() { return dagId; }
    public String getTaskId() { return taskId; }
    public LocalDate getLogicalDate() { return logicalDate; }
    public int getAttempt() { return attempt; }
}
