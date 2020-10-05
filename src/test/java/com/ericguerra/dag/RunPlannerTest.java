package com.ericguerra.dag;

import com.ericguerra.dag.engine.RunPlanner;
import com.ericguerra.dag.engine.TaskInstance;
import com.ericguerra.dag.model.DagDefinition;
import com.ericguerra.dag.model.TaskDefinition;
import com.ericguerra.dag.model.TaskState;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.ericguerra.dag.model.TaskDefinition.simple;
import static org.assertj.core.api.Assertions.assertThat;

class RunPlannerTest {
    static final Instant NOW = Instant.parse("2020-03-01T02:00:00Z");

    static Map<String, TaskInstance> fresh(DagDefinition d) {
        Map<String, TaskInstance> m = new LinkedHashMap<>();
        d.getTopoOrder().forEach(id -> m.put(id, new TaskInstance(id)));
        return m;
    }

    static TaskDefinition rule(String id, TaskDefinition.TriggerRule r, String... deps) {
        return new TaskDefinition(id, "x", null, List.of(deps), 0, null, null, r);
    }

    @Test
    void startsRootsWithinParallelismBudget() {
        DagDefinition d = new DagDefinition("p", null, 2, List.of(simple("a", "x"), simple("b", "x"), simple("c", "x"), simple("d", "x", "a")));
        Map<String, TaskInstance> t = fresh(d);
        assertThat(RunPlanner.plan(d, t, NOW)).containsExactly("a", "b");
        assertThat(RunPlanner.plan(d, t, NOW)).isEmpty(); // budget used by QUEUED
        t.get("a").setState(TaskState.SUCCESS);
        t.get("b").setState(TaskState.SUCCESS);
        assertThat(RunPlanner.plan(d, t, NOW)).containsExactly("c", "d");
    }

    @Test
    void failureCascadesAsUpstreamFailed() {
        DagDefinition d = new DagDefinition("p", null, 4, List.of(simple("a", "x"), simple("b", "x", "a"), simple("c", "x", "b"), simple("side", "x")));
        Map<String, TaskInstance> t = fresh(d);
        t.get("a").setState(TaskState.FAILED);
        t.get("side").setState(TaskState.SUCCESS);
        assertThat(RunPlanner.plan(d, t, NOW)).isEmpty();
        assertThat(t.get("b").getState()).isEqualTo(TaskState.UPSTREAM_FAILED);
        assertThat(t.get("c").getState()).isEqualTo(TaskState.UPSTREAM_FAILED);
        assertThat(RunPlanner.isFinished(t)).isTrue();
        assertThat(RunPlanner.isSuccessful(t)).isFalse();
    }

    @Test
    void triggerRulesForCleanupAndAlerting() {
        DagDefinition d = new DagDefinition("p", null, 4, List.of(
                simple("work", "x"),
                rule("cleanup", TaskDefinition.TriggerRule.ALL_DONE, "work"),
                rule("alert", TaskDefinition.TriggerRule.ONE_FAILED, "work")));

        Map<String, TaskInstance> ok = fresh(d);
        ok.get("work").setState(TaskState.SUCCESS);
        assertThat(RunPlanner.plan(d, ok, NOW)).containsExactly("cleanup");
        assertThat(ok.get("alert").getState()).isEqualTo(TaskState.SKIPPED);

        Map<String, TaskInstance> bad = fresh(d);
        bad.get("work").setState(TaskState.FAILED);
        assertThat(RunPlanner.plan(d, bad, NOW)).containsExactly("cleanup", "alert");
    }

    @Test
    void skippedPropagatesUnderAllSuccess() {
        DagDefinition d = new DagDefinition("p", null, 4, List.of(simple("work", "x"),
                rule("alert", TaskDefinition.TriggerRule.ONE_FAILED, "work"), simple("after_alert", "x", "alert")));
        Map<String, TaskInstance> t = fresh(d);
        t.get("work").setState(TaskState.SUCCESS);
        RunPlanner.plan(d, t, NOW);
        assertThat(t.get("after_alert").getState()).isEqualTo(TaskState.SKIPPED);
        assertThat(RunPlanner.isSuccessful(t)).isTrue();
    }

    @Test
    void retriesWaitUntilDue() {
        DagDefinition d = new DagDefinition("p", null, 4, List.of(simple("a", "x")));
        Map<String, TaskInstance> t = fresh(d);
        t.get("a").setState(TaskState.UP_FOR_RETRY);
        t.get("a").setNextAttemptAt(NOW.plus(Duration.ofMinutes(1)));
        assertThat(RunPlanner.plan(d, t, NOW)).isEmpty();
        assertThat(RunPlanner.plan(d, t, NOW.plus(Duration.ofMinutes(1)))).containsExactly("a");
    }
}
