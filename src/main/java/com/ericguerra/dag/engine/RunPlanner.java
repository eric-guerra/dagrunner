package com.ericguerra.dag.engine;

import com.ericguerra.dag.model.DagDefinition;
import com.ericguerra.dag.model.TaskDefinition;
import com.ericguerra.dag.model.TaskState;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Pure scheduling decisions for one run, separate from threads and I/O so it can be tested
 * deterministically. Given current task states, it resolves trigger rules (marking tasks
 * SKIPPED / UPSTREAM_FAILED) and returns what may start now, within the parallelism budget.
 */
public final class RunPlanner {
    private RunPlanner() {}

    public static List<String> plan(DagDefinition dag, Map<String, TaskInstance> tasks, Instant now) {
        // Resolve in topological order so a cascade (A failed → B upstream_failed → C upstream_failed) settles in one pass.
        for (String id : dag.getTopoOrder()) {
            TaskInstance ti = tasks.get(id);
            if (ti.getState() != TaskState.PENDING) continue;
            Decision d = evaluate(dag.task(id), tasks);
            if (d == Decision.SKIP) ti.setState(TaskState.SKIPPED);
            else if (d == Decision.UPSTREAM_FAILED) ti.setState(TaskState.UPSTREAM_FAILED);
        }

        long running = tasks.values().stream()
                .filter(t -> t.getState() == TaskState.RUNNING || t.getState() == TaskState.QUEUED).count();
        int budget = (int) Math.max(0, dag.getMaxParallelism() - running);

        List<String> start = new ArrayList<>();
        for (String id : dag.getTopoOrder()) {
            if (start.size() >= budget) break;
            TaskInstance ti = tasks.get(id);
            boolean pendingReady = ti.getState() == TaskState.PENDING && evaluate(dag.task(id), tasks) == Decision.RUN;
            boolean retryDue = ti.getState() == TaskState.UP_FOR_RETRY && !now.isBefore(ti.getNextAttemptAt());
            if (pendingReady || retryDue) {
                ti.setState(TaskState.QUEUED);
                start.add(id);
            }
        }
        return start;
    }

    public static boolean isFinished(Map<String, TaskInstance> tasks) {
        return tasks.values().stream().allMatch(t -> t.getState().isTerminal());
    }

    /** A run succeeds unless some task actually failed; skipped branches don't fail the run. */
    public static boolean isSuccessful(Map<String, TaskInstance> tasks) {
        return tasks.values().stream().noneMatch(t -> t.getState().isFailure());
    }

    enum Decision { WAIT, RUN, SKIP, UPSTREAM_FAILED }

    static Decision evaluate(TaskDefinition t, Map<String, TaskInstance> tasks) {
        boolean allTerminal = true, anyFailed = false, anySkipped = false, allSuccess = true;
        for (String dep : t.getDependsOn()) {
            TaskState s = tasks.get(dep).getState();
            if (!s.isTerminal()) allTerminal = false;
            if (s.isFailure()) anyFailed = true;
            if (s == TaskState.SKIPPED) anySkipped = true;
            if (s != TaskState.SUCCESS) allSuccess = false;
        }
        switch (t.getTriggerRule()) {
            case ALL_SUCCESS:
                if (anyFailed) return Decision.UPSTREAM_FAILED;
                if (anySkipped) return Decision.SKIP;
                return allSuccess ? Decision.RUN : Decision.WAIT;
            case ALL_DONE:
                return allTerminal ? Decision.RUN : Decision.WAIT;
            case ONE_FAILED:
                if (anyFailed) return Decision.RUN;
                return allTerminal ? Decision.SKIP : Decision.WAIT;
            default:
                throw new IllegalStateException();
        }
    }
}
