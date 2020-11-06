package com.ericguerra.dag;

import com.ericguerra.dag.engine.*;
import com.ericguerra.dag.model.DagDefinition;
import com.ericguerra.dag.model.TaskDefinition;
import com.ericguerra.dag.model.TaskState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class RunExecutorTest {
    static final LocalDate DS = LocalDate.of(2020, 3, 1);
    final ExecutorService pool = Executors.newFixedThreadPool(4);
    final List<String> transitions = Collections.synchronizedList(new ArrayList<>());
    final List<String> logs = Collections.synchronizedList(new ArrayList<>());

    final RunExecutor.RunListener listener = new RunExecutor.RunListener() {
        @Override public void taskChanged(long runId, TaskInstance ti) { transitions.add(ti.getTaskId() + ":" + ti.getState()); }
        @Override public void taskLog(long runId, String taskId, int attempt, String line) { logs.add(taskId + "#" + attempt + " " + line); }
    };

    @AfterEach
    void stop() { pool.shutdownNow(); }

    /** Operator whose behaviour per task is scripted by the test. */
    static class Scripted implements Operator {
        final Map<String, Deque<Object>> script = new ConcurrentHashMap<>();
        final Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();
        final AtomicInteger concurrent = new AtomicInteger();
        final AtomicInteger maxConcurrent = new AtomicInteger();

        Scripted on(String task, Object... outcomes) { script.put(task, new ArrayDeque<>(List.of(outcomes))); return this; }
        @Override public String type() { return "scripted"; }
        @Override public void execute(Map<String, Object> config, RunContext ctx) throws Exception {
            calls.computeIfAbsent(ctx.getTaskId(), k -> new AtomicInteger()).incrementAndGet();
            int c = concurrent.incrementAndGet();
            maxConcurrent.accumulateAndGet(c, Math::max);
            try {
                ctx.log(ctx.render("ds={{ ds }}"));
                Deque<Object> q = script.getOrDefault(ctx.getTaskId(), new ArrayDeque<>());
                Object next = q.isEmpty() ? "ok" : q.poll();
                if (next instanceof Long) Thread.sleep((Long) next);
                else if (next instanceof Exception) throw (Exception) next;
                else Thread.sleep(20);
            } finally {
                concurrent.decrementAndGet();
            }
        }
    }

    static TaskDefinition task(String id, int retries, Duration timeout, String... deps) {
        return new TaskDefinition(id, "scripted", null, List.of(deps), retries, Duration.ofMillis(10), timeout, null);
    }

    static Map<String, TaskInstance> fresh(DagDefinition d) {
        Map<String, TaskInstance> m = new LinkedHashMap<>();
        d.getTopoOrder().forEach(id -> m.put(id, new TaskInstance(id)));
        return m;
    }

    @Test
    void runsDiamondRespectingParallelism() throws Exception {
        DagDefinition d = new DagDefinition("diamond", null, 2, List.of(
                task("a", 0, null), task("b", 0, null, "a"), task("c", 0, null, "a"), task("e", 0, null, "a"), task("d", 0, null, "b", "c", "e")));
        Scripted op = new Scripted();
        RunExecutor ex = new RunExecutor(List.of(op), pool, Clock.systemUTC(), listener);
        Map<String, TaskInstance> tasks = fresh(d);
        assertThat(ex.run(1, d, DS, tasks)).isTrue();
        assertThat(tasks.values()).allMatch(t -> t.getState() == TaskState.SUCCESS);
        assertThat(op.maxConcurrent.get()).isLessThanOrEqualTo(2);
        assertThat(transitions.indexOf("d:RUNNING")).isGreaterThan(transitions.indexOf("b:SUCCESS"));
        assertThat(logs).contains("a#1 ds=2020-03-01");
    }

    @Test
    void retriesThenSucceeds() throws Exception {
        DagDefinition d = new DagDefinition("r", null, 1, List.of(task("flaky", 2, null)));
        Scripted op = new Scripted().on("flaky", new RuntimeException("503"), new RuntimeException("503"), "ok");
        RunExecutor ex = new RunExecutor(List.of(op), pool, Clock.systemUTC(), listener);
        Map<String, TaskInstance> tasks = fresh(d);
        assertThat(ex.run(1, d, DS, tasks)).isTrue();
        assertThat(tasks.get("flaky").getAttempts()).isEqualTo(3);
        assertThat(transitions).containsSubsequence("flaky:UP_FOR_RETRY", "flaky:UP_FOR_RETRY", "flaky:SUCCESS");
    }

    @Test
    void exhaustedRetriesFailAndCascade() throws Exception {
        DagDefinition d = new DagDefinition("f", null, 2, List.of(task("a", 1, null), task("b", 0, null, "a")));
        Scripted op = new Scripted().on("a", new RuntimeException("boom"), new RuntimeException("boom"));
        RunExecutor ex = new RunExecutor(List.of(op), pool, Clock.systemUTC(), listener);
        Map<String, TaskInstance> tasks = fresh(d);
        assertThat(ex.run(1, d, DS, tasks)).isFalse();
        assertThat(tasks.get("a").getState()).isEqualTo(TaskState.FAILED);
        assertThat(tasks.get("a").getLastError()).isEqualTo("RuntimeException: boom");
        assertThat(tasks.get("b").getState()).isEqualTo(TaskState.UPSTREAM_FAILED);
        assertThat(op.calls).doesNotContainKey("b");
    }

    @Test
    void timeoutCountsAsOneFailedAttempt() throws Exception {
        DagDefinition d = new DagDefinition("t", null, 1, List.of(task("slow", 1, Duration.ofMillis(100))));
        Scripted op = new Scripted().on("slow", 5_000L, "ok");
        RunExecutor ex = new RunExecutor(List.of(op), pool, Clock.systemUTC(), listener);
        Map<String, TaskInstance> tasks = fresh(d);
        assertThat(ex.run(1, d, DS, tasks)).isTrue();
        assertThat(tasks.get("slow").getAttempts()).isEqualTo(2);
    }

    @Test
    void resumesRunningTasksAfterCrash() throws Exception {
        DagDefinition d = new DagDefinition("c", null, 2, List.of(task("a", 0, null), task("b", 0, null, "a")));
        Map<String, TaskInstance> tasks = fresh(d);
        tasks.get("a").setState(TaskState.SUCCESS);
        tasks.get("b").setState(TaskState.RUNNING); // the scheduler died mid-task
        tasks.get("b").setAttempts(1);
        Scripted op = new Scripted();
        RunExecutor ex = new RunExecutor(List.of(op), pool, Clock.systemUTC(), listener);
        assertThat(ex.run(1, d, DS, tasks)).isTrue();
        assertThat(op.calls).containsOnlyKeys("b");
        assertThat(tasks.get("b").getAttempts()).isEqualTo(2);
    }
}
