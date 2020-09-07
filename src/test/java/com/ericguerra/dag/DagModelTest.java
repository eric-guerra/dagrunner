package com.ericguerra.dag;

import com.ericguerra.dag.model.DagDefinition;
import com.ericguerra.dag.model.DagLoader;
import com.ericguerra.dag.model.InvalidDagException;
import com.ericguerra.dag.model.TaskDefinition;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ericguerra.dag.model.TaskDefinition.simple;
import static org.assertj.core.api.Assertions.*;

class DagModelTest {
    @Test
    void topoOrderRespectsDependenciesAndDeclarationOrder() {
        DagDefinition d = new DagDefinition("etl", null, 2, List.of(
                simple("load", "x", "extract_a", "extract_b"),
                simple("extract_a", "x"),
                simple("extract_b", "x"),
                simple("report", "x", "load")));
        assertThat(d.getTopoOrder()).containsExactly("extract_a", "extract_b", "load", "report");
        assertThat(d.downstreamOf("extract_a")).containsExactly("load", "report");
    }

    @Test
    void rejectsCyclesUnknownDepsAndDuplicates() {
        assertThatThrownBy(() -> new DagDefinition("etl", null, 1, List.of(simple("a", "x", "c"), simple("b", "x", "a"), simple("c", "x", "b"))))
                .isInstanceOf(InvalidDagException.class).hasMessageContaining("cycle");
        assertThatThrownBy(() -> new DagDefinition("etl", null, 1, List.of(simple("a", "x", "nope"))))
                .hasMessageContaining("unknown task nope");
        assertThatThrownBy(() -> new DagDefinition("etl", null, 1, List.of(simple("a", "x"), simple("a", "x"))))
                .hasMessageContaining("duplicate");
        assertThatThrownBy(() -> new DagDefinition("Bad-Id", null, 1, List.of(simple("a", "x"))))
                .hasMessageContaining("bad dag id");
    }

    @Test
    void loadsYamlWithConfigAndPolicies() throws Exception {
        String yaml = "id: nightly\nschedule: \"0 0 2 * * ?\"\ntasks:\n"
                + "  - id: extract\n    type: http\n    url: http://x/{{ ds }}\n    retries: 2\n    retryDelay: 90s\n"
                + "  - id: alert\n    type: http\n    url: http://alerts\n    dependsOn: [extract]\n    triggerRule: one_failed\n    timeout: 5m\n";
        DagDefinition d = new DagLoader().parse(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)));
        TaskDefinition extract = d.task("extract");
        assertThat(extract.getConfig()).containsEntry("url", "http://x/{{ ds }}").doesNotContainKey("retries");
        assertThat(extract.getRetries()).isEqualTo(2);
        assertThat(extract.getRetryDelay()).isEqualTo(Duration.ofSeconds(90));
        assertThat(d.task("alert").getTriggerRule()).isEqualTo(TaskDefinition.TriggerRule.ONE_FAILED);
        assertThat(d.task("alert").getTimeout()).isEqualTo(Duration.ofMinutes(5));
    }

    @Test
    void directoryLoadIsolatesBadFiles() throws Exception {
        Path dir = Files.createTempDirectory("dags");
        Files.writeString(dir.resolve("good.yml"), "id: good\ntasks:\n  - id: a\n    type: shell\n");
        Files.writeString(dir.resolve("cyclic.yml"), "id: cyclic\ntasks:\n  - id: a\n    type: shell\n    dependsOn: [b]\n  - id: b\n    type: shell\n    dependsOn: [a]\n");
        Map<Path, String> errors = new HashMap<>();
        Map<String, DagDefinition> dags = new DagLoader().loadDirectory(dir, errors);
        assertThat(dags).containsOnlyKeys("good");
        assertThat(errors).hasSize(1);
        assertThat(errors.values().iterator().next()).contains("cycle");
    }

    @Test
    void bundledExampleDagIsValid() throws Exception {
        Map<Path, String> errors = new HashMap<>();
        Map<String, DagDefinition> dags = new DagLoader().loadDirectory(Path.of("dags"), errors);
        assertThat(errors).isEmpty();
        assertThat(dags.get("nightly_inventory").getTopoOrder()).startsWith("extract_pos", "extract_wms", "load_staging", "reconcile");
    }
}
