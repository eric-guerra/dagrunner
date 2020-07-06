package com.ericguerra.dag.operators;

import com.ericguerra.dag.engine.Operator;
import com.ericguerra.dag.engine.RunContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Map;

/**
 * Runs SQL against the warehouse in one transaction. Statements should be written idempotently
 * for the logical date, e.g. {@code DELETE ... WHERE ds = '{{ ds }}'; INSERT ... }.
 */
@Component
public class SqlOperator implements Operator {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;

    public SqlOperator(JdbcTemplate jdbc, TransactionTemplate tx) {
        this.jdbc = jdbc;
        this.tx = tx;
    }

    @Override
    public String type() { return "sql"; }

    @Override
    public void execute(Map<String, Object> config, RunContext ctx) {
        String sql = ctx.render(String.valueOf(config.get("sql")));
        tx.executeWithoutResult(status -> {
            for (String stmt : sql.split(";")) {
                if (stmt.isBlank()) continue;
                int n = jdbc.update(stmt.trim());
                ctx.log(n + " rows: " + stmt.trim().split("\\s+")[0]);
            }
        });
    }
}
