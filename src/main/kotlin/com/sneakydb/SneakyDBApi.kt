package com.sneakydb

import java.util.concurrent.CompletableFuture
import kotlin.jvm.JvmSuppressWildcards

/**
 * Stable Java/JDK-only API for one SneakyDB enable generation. Database work runs on workers.
 * Operations fail via their returned futures while initialization is pending, after failure,
 * after shutdown, or when worker capacity is exhausted. They never wait for readiness.
 * Completion callbacks may run inline; do not block them or access Bukkit state without rescheduling.
 *
 * Keep signatures to JDK types only (no Kotlin, Paper, JDBC or Hikari types) and add overloads rather than changing them;
 * consumers (SneakyJobBoard, SneakyPrototypeKit, LoMPhoneBridge) pin a version with compileOnly, so grep them first.
 */
interface SneakyDBApi {
    /** Executes a query and returns each row as a column-label/value map. */
    @JvmSuppressWildcards
    fun query(sql: String, parameters: List<*>): CompletableFuture<List<Map<String, Any?>>>

    /** Executes an insert, update, or delete and returns the affected row count. */
    @JvmSuppressWildcards
    fun execute(sql: String, parameters: List<*>): CompletableFuture<Int>

    /**
     * Completes after acquisition and successful finite-time connection validation, or exceptionally
     * with a credential-safe initialization failure / shutdown before readiness. No automatic retry.
     * Each call returns a defensive copy: caller completion or cancellation cannot change readiness.
     * Success is historical, not a health probe, and remains successful after shutdown. A stale API
     * never follows a new enable generation; reacquire it from the provider after re-enable.
     */
    fun whenReady(): CompletableFuture<Void>

    /** True after successful initialization until shutdown; not a health probe. */
    fun isAvailable(): Boolean
}
