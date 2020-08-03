package com.ericguerra.dag.store;

import com.ericguerra.dag.engine.RunExecutor;
import com.ericguerra.dag.engine.TaskInstance;
import com.ericguerra.dag.model.TaskState;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;

@Repository
public class RunRepository implements RunExecutor.RunListener {
    public static class RunRow {
        public long id;
        public String dagId;
        public LocalDate logicalDate;
        public String state;
        public String triggeredBy;
        public Instant createdAt;
        public Instant endedAt;
    }

    private final JdbcTemplate jdbc;

    public RunRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * One run per (dag, logical date): the unique key makes double-firing (two scheduler nodes,
     * or a manual trigger racing cron) a no-op instead of a duplicate load.
     */
    public Optional<Long> createRun(String dagId, LocalDate ds, String triggeredBy, Collection<String> taskIds, Instant now) {
        GeneratedKeyHolder key = new GeneratedKeyHolder();
        try {
            jdbc.update(con -> {
                PreparedStatement ps = con.prepareStatement(
                        "INSERT INTO dag_runs (dag_id, logical_date, state, triggered_by, created_at) VALUES (?, ?, 'RUNNING', ?, ?)",
                        new String[]{"id"});
                ps.setString(1, dagId);
                ps.setObject(2, ds);
                ps.setString(3, triggeredBy);
                ps.setTimestamp(4, Timestamp.from(now));
                return ps;
            }, key);
        } catch (DuplicateKeyException e) {
            return Optional.empty();
        }
        long runId = key.getKey().longValue();
        List<Object[]> rows = new ArrayList<>();
        for (String t : taskIds) rows.add(new Object[]{runId, t});
        jdbc.batchUpdate("INSERT INTO task_instances (run_id, task_id, state, attempts) VALUES (?, ?, 'PENDING', 0)", rows);
        return Optional.of(runId);
    }

    public Map<String, TaskInstance> loadTasks(long runId) {
        Map<String, TaskInstance> out = new LinkedHashMap<>();
        jdbc.query("SELECT task_id, state, attempts, next_attempt_at, started_at, ended_at, last_error FROM task_instances WHERE run_id = ? ORDER BY task_id",
                rs -> {
                    TaskInstance ti = new TaskInstance(rs.getString(1));
                    ti.setState(TaskState.valueOf(rs.getString(2)));
                    ti.setAttempts(rs.getInt(3));
                    ti.setNextAttemptAt(instant(rs.getTimestamp(4)));
                    ti.setStartedAt(instant(rs.getTimestamp(5)));
                    ti.setEndedAt(instant(rs.getTimestamp(6)));
                    ti.setLastError(rs.getString(7));
                    out.put(ti.getTaskId(), ti);
                }, runId);
        return out;
    }

    @Override
    public void taskChanged(long runId, TaskInstance ti) {
        jdbc.update("UPDATE task_instances SET state = ?, attempts = ?, next_attempt_at = ?, started_at = ?, ended_at = ?, last_error = ? WHERE run_id = ? AND task_id = ?",
                ti.getState().name(), ti.getAttempts(), ts(ti.getNextAttemptAt()), ts(ti.getStartedAt()), ts(ti.getEndedAt()), ti.getLastError(), runId, ti.getTaskId());
    }

    @Override
    public void taskLog(long runId, String taskId, int attempt, String line) {
        jdbc.update("INSERT INTO task_logs (run_id, task_id, attempt, line, at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)",
                runId, taskId, attempt, line.length() > 2000 ? line.substring(0, 2000) : line);
    }

    public void finishRun(long runId, boolean success, Instant now) {
        jdbc.update("UPDATE dag_runs SET state = ?, ended_at = ? WHERE id = ?", success ? "SUCCESS" : "FAILED", Timestamp.from(now), runId);
    }

    /** Clear a task and everything downstream so they run again within the same run. */
    public void resetTasks(long runId, Collection<String> taskIds) {
        for (String t : taskIds) {
            jdbc.update("UPDATE task_instances SET state = 'PENDING', next_attempt_at = NULL, ended_at = NULL, last_error = NULL WHERE run_id = ? AND task_id = ?", runId, t);
        }
        jdbc.update("UPDATE dag_runs SET state = 'RUNNING', ended_at = NULL WHERE id = ?", runId);
    }

    public List<Long> unfinishedRuns() {
        return jdbc.queryForList("SELECT id FROM dag_runs WHERE state = 'RUNNING' ORDER BY id", Long.class);
    }

    public Optional<RunRow> findRun(long runId) {
        List<RunRow> rows = jdbc.query("SELECT id, dag_id, logical_date, state, triggered_by, created_at, ended_at FROM dag_runs WHERE id = ?", (rs, i) -> {
            RunRow r = new RunRow();
            r.id = rs.getLong(1);
            r.dagId = rs.getString(2);
            r.logicalDate = rs.getObject(3, LocalDate.class);
            r.state = rs.getString(4);
            r.triggeredBy = rs.getString(5);
            r.createdAt = instant(rs.getTimestamp(6));
            r.endedAt = instant(rs.getTimestamp(7));
            return r;
        }, runId);
        return rows.stream().findFirst();
    }

    public List<Map<String, Object>> recentRuns(String dagId, int limit) {
        return jdbc.queryForList("SELECT id, logical_date, state, triggered_by, created_at, ended_at FROM dag_runs WHERE dag_id = ? ORDER BY logical_date DESC LIMIT ?", dagId, limit);
    }

    public List<String> logs(long runId, String taskId) {
        return jdbc.queryForList("SELECT line FROM task_logs WHERE run_id = ? AND task_id = ? ORDER BY id", String.class, runId, taskId);
    }

    private static Timestamp ts(Instant i) { return i == null ? null : Timestamp.from(i); }
    private static Instant instant(Timestamp t) { return t == null ? null : t.toInstant(); }
}
