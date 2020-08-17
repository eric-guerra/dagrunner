package com.ericguerra.dag.operators;

import com.ericguerra.dag.engine.Operator;
import com.ericguerra.dag.engine.RunContext;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/** Runs a command; non-zero exit fails the attempt. Output lines go to the task log. */
@Component
public class ShellOperator implements Operator {
    @Override
    public String type() { return "shell"; }

    @Override
    public void execute(Map<String, Object> config, RunContext ctx) throws Exception {
        String command = ctx.render(String.valueOf(config.get("command")));
        ProcessBuilder pb = new ProcessBuilder(List.of("/bin/sh", "-c", command)).redirectErrorStream(true);
        pb.environment().put("DS", ctx.getLogicalDate().toString());
        pb.environment().put("RUN_ID", Long.toString(ctx.getRunId()));
        Process p = pb.start();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = r.readLine()) != null) ctx.log(line);
        } catch (Exception e) {
            p.destroyForcibly();
            throw e;
        }
        int code = p.waitFor();
        if (code != 0) throw new IllegalStateException("exit code " + code);
    }
}
