package com.sneakydb;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Stable Java/JDK-only API for plugins using the configured database. Calls never block the server thread. */
public interface SneakyDBApi {
    /** Executes a query and returns each row as a column-label/value map. */
    CompletableFuture<List<Map<String, Object>>> query(String sql, List<?> parameters);

    /** Executes an insert, update, or delete and returns the affected row count. */
    CompletableFuture<Integer> execute(String sql, List<?> parameters);

    /** True after the configured adapter has opened its connection pool. */
    boolean isAvailable();
}
