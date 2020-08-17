package com.ericguerra.dag.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

/** Parses DAG YAML files. Durations accept "30s", "5m", "2h". */
public class DagLoader {
    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    public DagDefinition parse(InputStream in) throws IOException {
        JsonNode root = YAML.readTree(in);
        if (root == null || !root.has("id")) throw new InvalidDagException("dag file has no id");
        List<TaskDefinition> tasks = new ArrayList<>();
        for (JsonNode t : iterable(root.path("tasks"))) {
            Map<String, Object> config = new LinkedHashMap<>();
            Iterator<Map.Entry<String, JsonNode>> fields = t.fields();
            Set<String> reserved = Set.of("id", "type", "dependsOn", "retries", "retryDelay", "timeout", "triggerRule");
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> f = fields.next();
                if (!reserved.contains(f.getKey())) config.put(f.getKey(), YAML.convertValue(f.getValue(), Object.class));
            }
            List<String> deps = new ArrayList<>();
            for (JsonNode d : iterable(t.path("dependsOn"))) deps.add(d.asText());
            tasks.add(new TaskDefinition(
                    text(t, "id"), text(t, "type"), config, deps,
                    t.path("retries").asInt(0),
                    duration(text(t, "retryDelay")),
                    duration(text(t, "timeout")),
                    t.has("triggerRule") ? TaskDefinition.TriggerRule.valueOf(t.get("triggerRule").asText().toUpperCase()) : null));
        }
        return new DagDefinition(root.get("id").asText(), text(root, "schedule"), root.path("maxParallelism").asInt(4), tasks);
    }

    /** Load every *.yml / *.yaml in a folder; one broken file doesn't block the rest. */
    public Map<String, DagDefinition> loadDirectory(Path dir, Map<Path, String> errors) throws IOException {
        Map<String, DagDefinition> out = new TreeMap<>();
        if (!Files.isDirectory(dir)) return out;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(dir, "*.{yml,yaml}")) {
            for (Path f : files) {
                try (InputStream in = Files.newInputStream(f)) {
                    DagDefinition d = parse(in);
                    if (out.containsKey(d.getId())) throw new InvalidDagException("dag id " + d.getId() + " defined twice");
                    out.put(d.getId(), d);
                } catch (InvalidDagException | IOException e) {
                    errors.put(f, e.getMessage());
                }
            }
        }
        return out;
    }

    static Duration duration(String s) {
        if (s == null) return null;
        s = s.trim();
        long n = Long.parseLong(s.substring(0, s.length() - 1));
        switch (s.charAt(s.length() - 1)) {
            case 's': return Duration.ofSeconds(n);
            case 'm': return Duration.ofMinutes(n);
            case 'h': return Duration.ofHours(n);
            default: throw new InvalidDagException("bad duration " + s);
        }
    }

    private static String text(JsonNode n, String field) {
        return n.hasNonNull(field) ? n.get(field).asText() : null;
    }

    private static Iterable<JsonNode> iterable(JsonNode n) {
        return n.isArray() ? n::elements : Collections::emptyIterator;
    }
}
