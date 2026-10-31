package com.sneakydb

import com.zaxxer.hikari.HikariDataSource
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit

internal fun createHealthSource(settings: com.sneakydb.adapter.DatabaseSettings): HikariDataSource =
    com.sneakydb.adapter.PostgreSqlAdapter().createDataSource(
        settings.copy(maximumPoolSize = 1, connectionTimeoutMs = 5000),
    ).apply {
        poolName = "SneakyDB-Health"
        minimumIdle = 0
        validationTimeout = 5000
        addDataSourceProperty("socketTimeout", "5")
    }

/** One dedicated health task and one dedicated connection, never the consumer queue/pool. */
internal class HealthWorker(
    private val createSource: () -> HikariDataSource,
    private val intervalMs: Long,
) : AutoCloseable {
    private var source: HikariDataSource? = null // Owned exclusively by the health worker.
    private val executor = object : ScheduledThreadPoolExecutor(1, { task ->
        Thread(task, "SneakyDB-Health").apply { isDaemon = true }
    }) {
        override fun terminated() {
            try { source?.close() } catch (_: Exception) { /* Never log driver diagnostics/secrets. */ }
        }
    }.apply { removeOnCancelPolicy = true }

    fun schedule(check: () -> Unit): AutoCloseable {
        val task = executor.scheduleWithFixedDelay(check, 0, intervalMs, TimeUnit.MILLISECONDS)
        return AutoCloseable { task.cancel(true) }
    }

    fun probe(): Boolean {
        val pool = source ?: createSource().also { source = it }
        return pool.connection.use { it.isValid(5) }
    }

    private val closing = java.util.concurrent.atomic.AtomicBoolean()
    override fun close() {
        // ThreadPoolExecutor may invoke terminated() on its shutdown caller when idle.
        if (closing.compareAndSet(false, true)) Thread({ executor.shutdownNow() }, "SneakyDB-Health-Close")
            .apply { isDaemon = true }.start()
    }
    fun awaitTermination(timeout: Long, unit: TimeUnit): Boolean = executor.awaitTermination(timeout, unit)
}
