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
 * consumers pin a version with compileOnly, and many implement this interface as a test double, so grep them first:
 * lom-phone-app, resource-pipeline, SneakyCharacterManager-Paper, SneakyJobBoard, SneakyLeveling, SneakyMail, SneakyMisc,
 * SneakyMobile, SneakyPipeMonitor, SneakyPlots, SneakyPrototypeKit, SneakyRoamin. New methods need a default body.
 */
interface SneakyDBApi {
    /** Executes a query and returns each row as a column-label/value map. */
    @JvmSuppressWildcards
    fun query(sql: String, parameters: List<*>): CompletableFuture<List<Map<String, Any?>>>

    /** Executes an insert, update, or delete and returns the affected row count. */
    @JvmSuppressWildcards
    fun execute(sql: String, parameters: List<*>): CompletableFuture<Int>

    /**
     * Runs [work] in one database transaction on a worker that holds one pooled connection with autocommit off.
     * When [work] returns, the transaction commits and the future completes with its value; when it throws, the
     * transaction rolls back and the future completes exceptionally with that exception. A failed commit fails the
     * future too, which never completes normally unless the commit succeeded.
     *
     * The handle's calls block. It is valid only inside [work], on the worker thread. Calling [query], [execute] or
     * [transaction] from inside [work] fails with IllegalStateException; use the handle. Same admission rules as
     * [query]: fails through the future before readiness, after failure or shutdown, and when the workers are full.
     * Completing or cancelling the future before [work] returns rolls the transaction back (work is not interrupted).
     */
    fun <T> transaction(work: java.util.function.Function<SneakyDBTransaction, T>): CompletableFuture<T> =
        throw UnsupportedOperationException("This SneakyDB API does not support transactions")

    /**
     * Completes after acquisition and successful finite-time connection validation, or exceptionally
     * with a credential-safe initialization failure / shutdown before readiness. No automatic retry.
     * Each call returns a defensive copy: caller completion or cancellation cannot change readiness.
     * Success is historical, not a health probe, and remains successful after shutdown. A stale API
     * never follows a new enable generation; reacquire it from the provider after re-enable.
     */
    fun whenReady(): CompletableFuture<Void>

    /**
     * Registers [plugin]'s migration scripts; the future completes with the plugin's schema version once they are applied.
     *
     * [plugin] is the plugin's stable lower-case id (its `IDENTIFIER`, `[a-z][a-z0-9_]{0,62}`). The scripts are the files
     * `db/migrations/V<number>__<name>.sql` in the jar (or class directory) that [anchor] was loaded from, numbered from 1
     * without gaps. Call once per enable, during enable, and touch the plugin's tables only after the future completed.
     * SneakyDB runs pending scripts once every plugin had its chance to register (first server tick on Paper, after
     * proxy initialisation on Velocity), or at once for a later registration. The future fails with a message that says
     * what to do when the scripts are invalid, the database is newer than the build, a script fails, a requirement on
     * another plugin can't be met, or the database is unavailable. Completion callbacks may run on a SneakyDB thread.
     */
    fun migrate(plugin: String, anchor: Class<*>): CompletableFuture<Int> =
        throw UnsupportedOperationException("This SneakyDB API does not support migrations")

    /** True after successful initialization until shutdown; not a health probe. */
    fun isAvailable(): Boolean

    /** Same as the three-argument [listen] without a connected callback. */
    fun listen(channel: String, onNotification: java.util.function.Consumer<String>): AutoCloseable =
        listen(channel, onNotification) {}

    /**
     * Subscribes to database notifications (PostgreSQL `LISTEN`; send with `NOTIFY` or `pg_notify`).
     * [channel] must be a lower-case identifier (`[a-z_][a-z0-9_]{0,62}`). Callbacks run on SneakyDB's
     * notification thread: keep them short and reschedule before touching Bukkit state.
     *
     * Notifications sent while the connection is down are lost. [onConnected] runs each time this
     * subscription starts listening, including after a reconnect, so re-read the state you track there.
     * Close the result to unsubscribe. Calls before readiness are fine; throws IllegalStateException after
     * initialization failure or shutdown. Shutdown ends every subscription.
     */
    fun listen(channel: String, onNotification: java.util.function.Consumer<String>, onConnected: Runnable): AutoCloseable =
        throw UnsupportedOperationException("This SneakyDB API does not support notifications")
}
