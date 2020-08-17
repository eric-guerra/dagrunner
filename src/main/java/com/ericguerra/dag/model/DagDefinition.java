package com.ericguerra.dag.model;

import java.util.*;

/**
 * An immutable, validated DAG. Built from YAML like:
 *
 * <pre>
 * id: nightly_inventory
 * schedule: "0 0 2 * * ?"
 * tasks:
 *   - id: extract_pos
 *     type: shell
 *     command: ./extract.sh pos
 *   - id: load
 *     type: http
 *     url: http://loader/run
 *     dependsOn: [extract_pos]
 *     retries: 3
 * </pre>
 */
public final class DagDefinition {
    private final String id;
    private final String schedule;
    private final int maxParallelism;
    private final Map<String, TaskDefinition> tasks;
    private final List<String> topoOrder;

    public DagDefinition(String id, String schedule, int maxParallelism, List<TaskDefinition> taskList) {
        if (id == null || !id.matches("[a-z][a-z0-9_]{0,62}")) throw new InvalidDagException("bad dag id: " + id);
        if (taskList == null || taskList.isEmpty()) throw new InvalidDagException(id + " has no tasks");
        this.id = id;
        this.schedule = schedule;
        this.maxParallelism = maxParallelism <= 0 ? 4 : maxParallelism;

        Map<String, TaskDefinition> byId = new LinkedHashMap<>();
        for (TaskDefinition t : taskList) {
            if (byId.put(t.getId(), t) != null) throw new InvalidDagException("duplicate task " + t.getId());
        }
        for (TaskDefinition t : taskList) {
            for (String dep : t.getDependsOn()) {
                if (!byId.containsKey(dep)) throw new InvalidDagException(t.getId() + " depends on unknown task " + dep);
                if (dep.equals(t.getId())) throw new InvalidDagException(t.getId() + " depends on itself");
            }
        }
        this.tasks = Collections.unmodifiableMap(byId);
        this.topoOrder = Collections.unmodifiableList(topoSort(byId));
    }

    /** Kahn's algorithm; ties broken by declaration order so runs are reproducible. Reports the cycle if any. */
    private static List<String> topoSort(Map<String, TaskDefinition> tasks) {
        Map<String, Integer> indegree = new LinkedHashMap<>();
        Map<String, List<String>> downstream = new HashMap<>();
        for (TaskDefinition t : tasks.values()) {
            indegree.put(t.getId(), t.getDependsOn().size());
            for (String d : t.getDependsOn()) downstream.computeIfAbsent(d, k -> new ArrayList<>()).add(t.getId());
        }
        Deque<String> ready = new ArrayDeque<>();
        indegree.forEach((k, v) -> { if (v == 0) ready.add(k); });
        List<String> order = new ArrayList<>();
        while (!ready.isEmpty()) {
            String n = ready.poll();
            order.add(n);
            for (String m : downstream.getOrDefault(n, List.of())) {
                if (indegree.merge(m, -1, Integer::sum) == 0) ready.add(m);
            }
        }
        if (order.size() != tasks.size()) {
            List<String> stuck = new ArrayList<>();
            indegree.forEach((k, v) -> { if (v > 0) stuck.add(k); });
            throw new InvalidDagException("cycle among tasks " + stuck);
        }
        return order;
    }

    public Set<String> downstreamOf(String taskId) {
        Set<String> out = new LinkedHashSet<>();
        Deque<String> q = new ArrayDeque<>(List.of(taskId));
        while (!q.isEmpty()) {
            String cur = q.poll();
            for (TaskDefinition t : tasks.values()) {
                if (t.getDependsOn().contains(cur) && out.add(t.getId())) q.add(t.getId());
            }
        }
        return out;
    }

    public String getId() { return id; }
    public String getSchedule() { return schedule; }
    public int getMaxParallelism() { return maxParallelism; }
    public Map<String, TaskDefinition> getTasks() { return tasks; }
    public TaskDefinition task(String id) { return tasks.get(id); }
    public List<String> getTopoOrder() { return topoOrder; }
}
