package com.ericguerra.dag.operators;

import com.ericguerra.dag.engine.Operator;
import com.ericguerra.dag.engine.RunContext;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/** Calls an endpoint (Java 11 HttpClient). 2xx succeeds; the run id is sent as an idempotency key. */
@Component
public class HttpOperator implements Operator {
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    @Override
    public String type() { return "http"; }

    @Override
    public void execute(Map<String, Object> config, RunContext ctx) throws Exception {
        String method = String.valueOf(config.getOrDefault("method", "POST")).toUpperCase();
        String body = config.containsKey("body") ? ctx.render(String.valueOf(config.get("body"))) : "";
        HttpRequest req = HttpRequest.newBuilder(URI.create(ctx.render(String.valueOf(config.get("url")))))
                .method(method, body.isEmpty() ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                .header("Content-Type", "application/json")
                .header("Idempotency-Key", ctx.getDagId() + ":" + ctx.getTaskId() + ":" + ctx.getLogicalDate())
                .timeout(Duration.ofMinutes(5))
                .build();
        HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
        ctx.log(method + " " + req.uri() + " -> " + res.statusCode());
        if (res.statusCode() / 100 != 2) throw new IllegalStateException("HTTP " + res.statusCode() + ": " + abbreviate(res.body()));
    }

    private static String abbreviate(String s) {
        return s.length() > 300 ? s.substring(0, 300) + "…" : s;
    }
}
