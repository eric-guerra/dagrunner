package com.ericguerra.dag.engine;

import com.ericguerra.dag.model.DagDefinition;
import com.ericguerra.dag.model.TaskDefinition;
import com.ericguerra.dag.model.TaskState;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.*;

/**
 * Drives one DAG run to completion: asks the planner what can start, submits those to a worker pool
 * with per-task timeouts, applies retry policy on failure, and persists every state change through
 * {@link RunListener} so a crashed scheduler can resume (RUNNING tasks are reset to UP_FOR_RETRY).
 */
public class RunExecutor {
    public interface RunListener {
        void taskChanged(long runId, TaskInstance ti);
        void taskLog(long runId, String taskId, int attempt, String line);
    }

    private final Map<String, Operator> operators;
    private final ExecutorService workers;
    private final ScheduledExecutorService timeouts = Executors.newSingleThreadScheduledExecutor();
    private final Clock clock;
    private final RunListener listener;

    public RunExecutor(Collection<Operator> operators, ExecutorService workers, Clock clock, RunListener listener) {
        this.operators = new HashMap<>();
        for (Operator o : operators) this.operators.put(o.type(), o);
        this.workers = workers;
        this.clock = clock;
        this.listener = listener;
    }

    private static final class Outcome {
        final String taskId;
        final int attempt;
        final Throwable error;
        Outcome(String taskId, int attempt, Throwable error) { this.taskId = taskId; this.attempt = attempt; this.error = error; }
    }

    /** Blocks until the run finishes; returns true if no task failed. */
    public boolean run(long runId, DagDefinition dag, LocalDate logicalDate, Map<String, TaskInstance> tasks) throws InterruptedException {
        // Resume semantics: anything mid-flight when we died gets another attempt.
        for (TaskInstance ti : tasks.values()) {
            if (ti.getState() == TaskState.RUNNING || ti.getState() == TaskState.QUEUED) {
                ti.setState(TaskState.UP_FOR_RETRY);
                ti.setNextAttemptAt(clock.instant());
                listener.taskChanged(runId, ti);
            }
        }

        BlockingQueue<Outcome> done = new LinkedBlockingQueue<>();
        int inFlight = 0;
        while (true) {
            Map<String, TaskState> before = snapshot(tasks);
            List<String> start = RunPlanner.plan(dag, tasks, clock.instant());
            notifyChanged(runId, tasks, before, start);

            for (String id : start) {
                launch(runId, dag.getId(), dag.task(id), tasks.get(id), logicalDate, done);
                inFlight++;
            }
            if (inFlight == 0 && RunPlanner.isFinished(tasks)) return RunPlanner.isSuccessful(tasks);

            Outcome o = done.poll(waitFor(tasks).toMillis(), TimeUnit.MILLISECONDS);
            if (o == null) continue; // a retry came due
            inFlight--;
            complete(runId, dag.task(o.taskId), tasks.get(o.taskId), o.error);
        }
    }

    private void launch(long runId, String dagId, TaskDefinition def, TaskInstance ti, LocalDate ds, BlockingQueue<Outcome> done) {
        ti.setAttempts(ti.getAttempts() + 1);
        ti.setState(TaskState.RUNNING);
        ti.setStartedAt(clock.instant());
        ti.setEndedAt(null);
        listener.taskChanged(runId, ti);

        Operator op = operators.get(def.getType());
        int attempt = ti.getAttempts();
        RunContext ctx = new RunContext(runId, dagId, def.getId(), ds, attempt, line -> listener.taskLog(runId, def.getId(), attempt, line));
        if (op == null) {
            done.add(new Outcome(def.getId(), attempt, new IllegalStateException("no operator for type " + def.getType())));
            return;
        }
        // Exactly one outcome per attempt: whichever of completion or timeout reports first wins.
        java.util.concurrent.atomic.AtomicBoolean reported = new java.util.concurrent.atomic.AtomicBoolean();
        Future<?> f = workers.submit(() -> {
            Throwable err = null;
            try {
                op.execute(def.getConfig(), ctx);
            } catch (Throwable t) {
                err = t;
            }
            if (reported.compareAndSet(false, true)) done.add(new Outcome(def.getId(), attempt, err));
        });
        timeouts.schedule(() -> {
            if (reported.compareAndSet(false, true)) {
                f.cancel(true);
                done.add(new Outcome(def.getId(), attempt, new TimeoutException("timed out after " + def.getTimeout())));
            }
        }, def.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
    }

    private void complete(long runId, TaskDefinition def, TaskInstance ti, Throwable error) {
        if (ti.getState() != TaskState.RUNNING) return; // late result after a timeout already counted
        ti.setEndedAt(clock.instant());
        if (error == null) {
            ti.setState(TaskState.SUCCESS);
            ti.setLastError(null);
        } else {
            ti.setLastError(error.getClass().getSimpleName() + ": " + error.getMessage());
            if (ti.getAttempts() <= def.getRetries()) {
                ti.setState(TaskState.UP_FOR_RETRY);
                // exponential backoff: delay, 2*delay, 4*delay…
                long factor = 1L << Math.min(10, ti.getAttempts() - 1);
                ti.setNextAttemptAt(clock.instant().plus(def.getRetryDelay().multipliedBy(factor)));
            } else {
                ti.setState(TaskState.FAILED);
            }
        }
        listener.taskChanged(runId, ti);
    }

    private Duration waitFor(Map<String, TaskInstance> tasks) {
        Instant now = clock.instant();
        Duration wait = Duration.ofSeconds(5);
        for (TaskInstance ti : tasks.values()) {
            if (ti.getState() == TaskState.UP_FOR_RETRY) {
                Duration d = Duration.between(now, ti.getNextAttemptAt());
                if (d.compareTo(wait) < 0) wait = d;
            }
        }
        return wait.isNegative() ? Duration.ZERO : wait;
    }

    private static Map<String, TaskState> snapshot(Map<String, TaskInstance> tasks) {
        Map<String, TaskState> m = new HashMap<>();
        tasks.forEach((k, v) -> m.put(k, v.getState()));
        return m;
    }

    private void notifyChanged(long runId, Map<String, TaskInstance> tasks, Map<String, TaskState> before, List<String> starting) {
        for (TaskInstance ti : tasks.values()) {
            if (before.get(ti.getTaskId()) != ti.getState() && !starting.contains(ti.getTaskId())) listener.taskChanged(runId, ti);
        }
    }

    public void shutdown() {
        timeouts.shutdownNow();
    }
}
