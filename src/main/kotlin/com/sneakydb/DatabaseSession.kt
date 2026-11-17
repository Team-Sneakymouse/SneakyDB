package com.sneakydb

import com.zaxxer.hikari.HikariDataSource
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService

/**
 * A single enable generation, never reused after shutdown. Errors reaching futures or logs must be credential-safe:
 * wrap driver exceptions in a generic IllegalStateException, never log the password or a JDBC URL with credentials.
 * The worker executor and Hikari pool must shut down on disable.
 */
internal class DatabaseSession(
    private val executor: ExecutorService,
    private val notifications: NotificationWorker? = null,
    migrations: ((DatabaseSession) -> MigrationCoordinator)? = null,
    createSource: () -> HikariDataSource,
) : SneakyDBApi {
    private val lock = Any()
    private val ready = CompletableFuture<Void>()
    private var source: HikariDataSource? = null
    private var failure: IllegalStateException? = null
    private var stopped = false
    private val operations = HashSet<CompletableFuture<*>>()
    /** Set on a worker while it runs a transaction, to reject calls that would need a second connection. */
    private val insideTransaction = ThreadLocal<Boolean>()
    /** This generation's migrations; null in sessions built without them (tests). */
    internal val migrations: MigrationCoordinator? = migrations?.invoke(this)

    init {
        executor.execute {
            var candidate: HikariDataSource? = null
            var stage = "adapter creation"
            try {
                candidate = createSource()
                stage = "connection acquisition"
                candidate.connection.use {
                    stage = "connection validation"
                    check(it.isValid(5))
                }
                val publish = synchronized(lock) {
                    if (stopped) false else { source = candidate; true }
                }
                if (publish) ready.complete(null) else candidate.close()
            } catch (_: Exception) {
                candidate?.let { runCatching { it.close() } }
                val error = IllegalStateException("SneakyDB initialization failed during $stage; check adapter configuration and database access")
                synchronized(lock) { failure = error }
                ready.completeExceptionally(error)
            }
        }
    }

    override fun migrate(plugin: String, anchor: Class<*>): CompletableFuture<Int> =
        migrations?.register(plugin, anchor)
            ?: CompletableFuture.failedFuture(UnsupportedOperationException("This SneakyDB session does not run migrations"))

    override fun whenReady(): CompletableFuture<Void> = ready.copy()
    override fun isAvailable(): Boolean = synchronized(lock) { source != null && !stopped }

    override fun listen(channel: String, onNotification: java.util.function.Consumer<String>, onConnected: Runnable): AutoCloseable {
        synchronized(lock) {
            failure?.let { throw it }
            check(!stopped) { "SneakyDB has shut down" }
        }
        val worker = notifications ?: throw UnsupportedOperationException("This SneakyDB adapter does not support notifications")
        return worker.subscribe(channel, onNotification, onConnected)
    }

    override fun query(sql: String, parameters: List<*>): CompletableFuture<List<Map<String, Any?>>> = submit { pool, _ ->
        pool.connection.use { readRows(it, sql, parameters) }
    }

    override fun execute(sql: String, parameters: List<*>): CompletableFuture<Int> = submit { pool, _ ->
        pool.connection.use { executeUpdate(it, sql, parameters) }
    }

    override fun <T> transaction(work: java.util.function.Function<SneakyDBTransaction, T>): CompletableFuture<T> =
        inTransaction { scope ->
            val handle = TransactionHandle(scope)
            try { work.apply(handle) } finally { handle.close() }
        }

    /**
     * Runs [block] on a worker in one transaction on a dedicated pooled connection, with the admission, shutdown and
     * cleanup rules of [transaction]. For SneakyDB's own code, which may use the raw connection; consumers use [transaction].
     */
    internal fun <T> inTransaction(block: (TransactionScope) -> T): CompletableFuture<T> = submit { pool, abandoned ->
        insideTransaction.set(true)
        try { runTransaction(pool, abandoned, block) } finally { insideTransaction.remove() }
    }

    /** [operation] gets the pool and a check that turns true once shutdown or completion of the future abandoned the work. */
    private fun <T> submit(operation: (HikariDataSource, () -> Boolean) -> T): CompletableFuture<T> {
        val result = CompletableFuture<T>()
        if (insideTransaction.get() == true) {
            // The calling worker holds a transaction: another connection can't see or join it, and waiting on one can
            // starve the four workers or deadlock on the transaction's own row locks.
            result.completeExceptionally(IllegalStateException(
                "SneakyDB query, execute and transaction can't be called inside a transaction's work; use its transaction handle"))
            return result
        }
        val selected = synchronized(lock) {
            if (stopped || failure != null) null else source?.also { operations.add(result) }
        }
        result.whenComplete { _, _ -> synchronized(lock) { operations.remove(result) } }
        if (selected == null) {
            result.completeExceptionally(synchronized(lock) { failure } ?: IllegalStateException("SneakyDB is not ready or has shut down"))
            return result
        }
        val abandoned = { synchronized(lock) { stopped } || result.isDone }
        try {
            executor.execute {
                // Claim work atomically with shutdown; JDBC runs outside the lifecycle lock.
                val admitted = synchronized(lock) { !stopped && !result.isDone }
                if (!admitted) return@execute
                try { result.complete(operation(selected, abandoned)) }
                // Throwable, not Exception: an Error from a transaction's work must still settle its future.
                catch (error: Throwable) { result.completeExceptionally(error) }
            }
        } catch (error: java.util.concurrent.RejectedExecutionException) {
            result.completeExceptionally(IllegalStateException("SneakyDB worker unavailable: shut down or queue capacity exhausted", error))
        }
        return result
    }

    fun close() {
        val (closing, pending) = synchronized(lock) {
            stopped = true
            (source to operations.toList()).also { source = null; operations.clear() }
        }
        val error = IllegalStateException("SneakyDB has shut down")
        ready.completeExceptionally(error)
        pending.forEach { it.completeExceptionally(error) }
        migrations?.close()
        executor.shutdownNow()
        notifications?.close()
        closing?.let { pool -> Thread({ pool.close() }, "SneakyDB-Close").apply { isDaemon = true }.start() }
    }
}
