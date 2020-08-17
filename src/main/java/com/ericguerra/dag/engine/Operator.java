package com.ericguerra.dag.engine;

import java.util.Map;

/** A task type. Implementations must be safe to retry: a run can be resumed after a crash. */
public interface Operator {
    String type();

    /** Throw to fail the attempt. The context holds run params, e.g. {@code ds} (logical date). */
    void execute(Map<String, Object> config, RunContext context) throws Exception;
}
