package com.ericguerra.dag.service;

import com.ericguerra.dag.engine.Operator;
import com.ericguerra.dag.engine.RunExecutor;
import com.ericguerra.dag.engine.TaskInstance;
import com.ericguerra.dag.model.DagDefinition;
import com.ericguerra.dag.model.DagLoader;
import com.ericguerra.dag.store.RunRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.*;

@Service
public class DagService {
    private static final Logger log = LoggerFactory.getLogger(DagService.class);

    private final RunRepository runs;
    private final Clock clock;
    private final Path dagDir;
    private final ExecutorService runThreads = Executors.newCachedThreadPool();
    private final RunExecutor executor;
    private volatile Map<String, DagDefinition> dags = Map.of();
    private volatile Map<Path, String> loadErrors = Map.of();
    private final Set<Long> active = ConcurrentHashMap.newKeySet();

    public DagService(RunRepository runs, List<Operator> operators, Clock clock,
                      @Value("${dagrunner.dag-dir:dags}") String dagDir,
                      @Value("${dagrunner.worker-threads:16}") int workerThreads) {
        this.runs = runs;
        this.clock = clock;
        this.dagDir = Path.of(dagDir);
        this.executor = new RunExecutor(operators, Executors.newFixedThreadPool(workerThreads), clock, runs);
    }

    @PostConstruct
    public void init() throws IOException {
        reload();
        // Resume runs that were in flight when the previous process died.
        for (Long id : runs.unfinishedRuns()) runs.findRun(id).ifPresent(r -> {
            DagDefinition d = dags.get(r.dagId);
            if (d != null) {
                log.info("resuming run {} of {}", id, r.dagId);
                submit(id, d, r.logicalDate);
            }
        });
    }

    public synchronized Map<Path, String> reload() throws IOException {
        Map<Path, String> errors = new LinkedHashMap<>();
        dags = new DagLoader().loadDirectory(dagDir, errors);
        loadErrors = errors;
        errors.forEach((f, e) -> log.warn("dag file {} rejected: {}", f, e));
        return errors;
    }

    public Collection<DagDefinition> dags() { return dags.values(); }
    public Map<Path, String> loadErrors() { return loadErrors; }

    public DagDefinition dag(String id) {
        DagDefinition d = dags.get(id);
        if (d == null) throw new NoSuchElementException("no dag " + id);
        return d;
    }

    /** Returns the run id, or empty if a run for that logical date already exists. */
    public Optional<Long> trigger(String dagId, LocalDate ds, String by) {
        DagDefinition d = dag(dagId);
        Optional<Long> id = runs.createRun(dagId, ds, by, d.getTopoOrder(), clock.instant());
        id.ifPresent(runId -> submit(runId, d, ds));
        return id;
    }

    /** One run per day in [from, to], oldest first, at most `concurrency` at a time. */
    public List<Long> backfill(String dagId, LocalDate from, LocalDate to, int concurrency, String by) {
        if (to.isBefore(from)) throw new IllegalArgumentException("to before from");
        DagDefinition d = dag(dagId);
        List<Long> created = new ArrayList<>();
        Semaphore permits = new Semaphore(Math.max(1, concurrency));
        for (LocalDate ds = from; !ds.isAfter(to); ds = ds.plusDays(1)) {
            LocalDate day = ds;
            runs.createRun(dagId, day, by, d.getTopoOrder(), clock.instant()).ifPresent(runId -> {
                created.add(runId);
                runThreads.submit(() -> {
                    permits.acquireUninterruptibly();
                    try { execute(runId, d, day); } finally { permits.release(); }
                });
            });
        }
        return created;
    }

    /** Re-run a task (and optionally everything downstream of it) inside an existing run. */
    public void clear(long runId, String taskId, boolean downstream) {
        RunRepository.RunRow r = runs.findRun(runId).orElseThrow(() -> new NoSuchElementException("no run " + runId));
        if (active.contains(runId)) throw new IllegalStateException("run " + runId + " is still executing");
        DagDefinition d = dag(r.dagId);
        if (d.task(taskId) == null) throw new NoSuchElementException("no task " + taskId);
        Set<String> reset = new LinkedHashSet<>();
        reset.add(taskId);
        if (downstream) reset.addAll(d.downstreamOf(taskId));
        runs.resetTasks(runId, reset);
        submit(runId, d, r.logicalDate);
    }

    private void submit(long runId, DagDefinition d, LocalDate ds) {
        runThreads.submit(() -> execute(runId, d, ds));
    }

    private void execute(long runId, DagDefinition d, LocalDate ds) {
        if (!active.add(runId)) return;
        try {
            Map<String, TaskInstance> tasks = runs.loadTasks(runId);
            boolean ok = executor.run(runId, d, ds, tasks);
            runs.finishRun(runId, ok, clock.instant());
            log.info("run {} of {} for {} finished: {}", runId, d.getId(), ds, ok ? "SUCCESS" : "FAILED");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            log.error("run {} crashed", runId, e);
            runs.finishRun(runId, false, clock.instant());
        } finally {
            active.remove(runId);
        }
    }

    public boolean isActive(long runId) { return active.contains(runId); }

    @PreDestroy
    public void shutdown() {
        runThreads.shutdownNow();
        executor.shutdown();
    }
}
