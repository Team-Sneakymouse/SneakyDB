package com.sneakydb

import com.zaxxer.hikari.HikariDataSource
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService

/** A single enable generation, never reused after shutdown. */
internal class DatabaseSession(
    private val executor: ExecutorService,
    createSource: () -> HikariDataSource,
) : SneakyDBApi {
    private val lock = Any()
    private val ready = CompletableFuture<Void>()
    private var source: HikariDataSource? = null
    private var failure: IllegalStateException? = null
    private var stopped = false
    private val operations = HashSet<CompletableFuture<*>>()

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

    override fun whenReady(): CompletableFuture<Void> = ready.copy()
    override fun isAvailable(): Boolean = synchronized(lock) { source != null && !stopped }

    override fun query(sql: String, parameters: List<*>): CompletableFuture<List<Map<String, Any?>>> = submit { pool ->
        pool.connection.use { connection ->
            connection.prepareStatement(sql).use { statement ->
                parameters.forEachIndexed { index, value -> statement.setObject(index + 1, value) }
                statement.executeQuery().use { result ->
                    val metadata = result.metaData
                    val rows = ArrayList<Map<String, Any?>>()
                    while (result.next()) {
                        val row = LinkedHashMap<String, Any?>()
                        for (column in 1..metadata.columnCount) row[metadata.getColumnLabel(column)] = result.getObject(column)
                        rows.add(java.util.Collections.unmodifiableMap(row))
                    }
                    java.util.Collections.unmodifiableList(rows)
                }
            }
        }
    }

    override fun execute(sql: String, parameters: List<*>): CompletableFuture<Int> = submit { pool ->
        pool.connection.use { connection ->
            connection.prepareStatement(sql).use { statement ->
                parameters.forEachIndexed { index, value -> statement.setObject(index + 1, value) }
                statement.executeUpdate()
            }
        }
    }

    private fun <T> submit(operation: (HikariDataSource) -> T): CompletableFuture<T> {
        val result = CompletableFuture<T>()
        val selected = synchronized(lock) {
            if (stopped || failure != null) null else source?.also { operations.add(result) }
        }
        result.whenComplete { _, _ -> synchronized(lock) { operations.remove(result) } }
        if (selected == null) {
            result.completeExceptionally(synchronized(lock) { failure } ?: IllegalStateException("SneakyDB is not ready or has shut down"))
            return result
        }
        try {
            executor.execute {
                // Claim work atomically with shutdown; JDBC runs outside the lifecycle lock.
                val admitted = synchronized(lock) { !stopped && !result.isDone }
                if (!admitted) return@execute
                try { result.complete(operation(selected)) }
                catch (error: Exception) { result.completeExceptionally(error) }
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
        executor.shutdownNow()
        closing?.let { pool -> Thread({ pool.close() }, "SneakyDB-Close").apply { isDaemon = true }.start() }
    }
}
