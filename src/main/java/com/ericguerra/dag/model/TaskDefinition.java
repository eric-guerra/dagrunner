package com.ericguerra.dag.model;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class TaskDefinition {
    public enum TriggerRule { ALL_SUCCESS, ALL_DONE, ONE_FAILED }

    private final String id;
    private final String type;
    private final Map<String, Object> config;
    private final List<String> dependsOn;
    private final int retries;
    private final Duration retryDelay;
    private final Duration timeout;
    private final TriggerRule triggerRule;

    public TaskDefinition(String id, String type, Map<String, Object> config, List<String> dependsOn,
                          int retries, Duration retryDelay, Duration timeout, TriggerRule triggerRule) {
        if (id == null || !id.matches("[a-z][a-z0-9_]{0,62}")) throw new InvalidDagException("bad task id: " + id);
        if (type == null) throw new InvalidDagException(id + " has no type");
        this.id = id;
        this.type = type;
        this.config = config == null ? Map.of() : Collections.unmodifiableMap(config);
        this.dependsOn = dependsOn == null ? List.of() : List.copyOf(dependsOn);
        this.retries = Math.max(0, retries);
        this.retryDelay = retryDelay == null ? Duration.ofSeconds(30) : retryDelay;
        this.timeout = timeout == null ? Duration.ofHours(1) : timeout;
        this.triggerRule = triggerRule == null ? TriggerRule.ALL_SUCCESS : triggerRule;
    }

    public static TaskDefinition simple(String id, String type, String... deps) {
        return new TaskDefinition(id, type, Map.of(), List.of(deps), 0, Duration.ZERO, null, null);
    }

    public String getId() { return id; }
    public String getType() { return type; }
    public Map<String, Object> getConfig() { return config; }
    public List<String> getDependsOn() { return dependsOn; }
    public int getRetries() { return retries; }
    public Duration getRetryDelay() { return retryDelay; }
    public Duration getTimeout() { return timeout; }
    public TriggerRule getTriggerRule() { return triggerRule; }
}
