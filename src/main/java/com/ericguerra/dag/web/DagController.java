package com.ericguerra.dag.web;

import com.ericguerra.dag.engine.TaskInstance;
import com.ericguerra.dag.model.DagDefinition;
import com.ericguerra.dag.model.InvalidDagException;
import com.ericguerra.dag.service.DagService;
import com.ericguerra.dag.store.RunRepository;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api")
public class DagController {
    private final DagService dags;
    private final RunRepository runs;

    public DagController(DagService dags, RunRepository runs) {
        this.dags = dags;
        this.runs = runs;
    }

    @GetMapping("/dags")
    public List<Map<String, Object>> list() {
        return dags.dags().stream().map(d -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", d.getId());
            m.put("schedule", d.getSchedule());
            m.put("tasks", d.getTopoOrder());
            return m;
        }).collect(Collectors.toList());
    }

    /** Edges for the UI graph view. */
    @GetMapping("/dags/{id}/graph")
    public Map<String, Object> graph(@PathVariable String id) {
        DagDefinition d = dags.dag(id);
        List<Map<String, String>> edges = new ArrayList<>();
        d.getTasks().values().forEach(t -> t.getDependsOn().forEach(dep -> edges.add(Map.of("from", dep, "to", t.getId()))));
        return Map.of("nodes", d.getTopoOrder(), "edges", edges);
    }

    @PostMapping("/dags/reload")
    public Map<String, String> reload() throws IOException {
        Map<String, String> out = new LinkedHashMap<>();
        dags.reload().forEach((f, e) -> out.put(f.getFileName().toString(), e));
        return out;
    }

    @PostMapping("/dags/{id}/runs")
    public ResponseEntity<Map<String, Object>> trigger(@PathVariable String id,
                                                       @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate ds) {
        return dags.trigger(id, ds, "api")
                .map(runId -> ResponseEntity.status(HttpStatus.CREATED).body(Map.<String, Object>of("runId", runId)))
                .orElseGet(() -> ResponseEntity.status(HttpStatus.CONFLICT).body(Map.<String, Object>of("error", "a run for " + ds + " already exists")));
    }

    @PostMapping("/dags/{id}/backfill")
    public Map<String, Object> backfill(@PathVariable String id,
                                        @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
                                        @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to,
                                        @RequestParam(defaultValue = "2") int concurrency) {
        return Map.of("runIds", dags.backfill(id, from, to, concurrency, "backfill"));
    }

    @GetMapping("/dags/{id}/runs")
    public List<Map<String, Object>> runs(@PathVariable String id, @RequestParam(defaultValue = "30") int limit) {
        return runs.recentRuns(id, Math.min(limit, 500));
    }

    @GetMapping("/runs/{runId}")
    public Map<String, Object> run(@PathVariable long runId) {
        RunRepository.RunRow r = runs.findRun(runId).orElseThrow(() -> new NoSuchElementException("no run " + runId));
        Map<String, TaskInstance> tasks = runs.loadTasks(runId);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("run", r);
        m.put("active", dags.isActive(runId));
        m.put("tasks", tasks.values());
        return m;
    }

    @PostMapping("/runs/{runId}/tasks/{taskId}/clear")
    public ResponseEntity<Void> clear(@PathVariable long runId, @PathVariable String taskId, @RequestParam(defaultValue = "true") boolean downstream) {
        dags.clear(runId, taskId, downstream);
        return ResponseEntity.accepted().build();
    }

    @GetMapping("/runs/{runId}/tasks/{taskId}/logs")
    public List<String> logs(@PathVariable long runId, @PathVariable String taskId) {
        return runs.logs(runId, taskId);
    }

    @ExceptionHandler(NoSuchElementException.class)
    public ResponseEntity<Map<String, String>> notFound(NoSuchElementException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler({IllegalStateException.class, IllegalArgumentException.class, InvalidDagException.class})
    public ResponseEntity<Map<String, String>> conflict(RuntimeException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.getMessage()));
    }
}
