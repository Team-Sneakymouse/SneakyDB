package com.sneakydb

import java.util.concurrent.CompletableFuture
import kotlin.jvm.JvmSuppressWildcards

/** Stable Java/JDK-only API for plugins using the configured database. Calls never block the server thread. */
interface SneakyDBApi {
    /** Executes a query and returns each row as a column-label/value map. */
    @JvmSuppressWildcards
    fun query(sql: String, parameters: List<*>): CompletableFuture<List<Map<String, Any?>>>

    /** Executes an insert, update, or delete and returns the affected row count. */
    @JvmSuppressWildcards
    fun execute(sql: String, parameters: List<*>): CompletableFuture<Int>

    /** True after the configured adapter has opened its connection pool. */
    fun isAvailable(): Boolean
}
