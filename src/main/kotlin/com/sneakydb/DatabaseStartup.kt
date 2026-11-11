package com.sneakydb

import com.sneakydb.adapter.DatabaseSettings
import com.sneakydb.adapter.PostgreSqlAdapter
import com.sneakydb.adapter.openPostgreSqlNotificationConnection
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

// Shared by the Paper and Velocity entry points: no Bukkit or Velocity types in this file.

/** The `database:` config section, read with the same keys, defaults and bounds on every platform. */
internal class DatabaseConfig(val adapterId: String, val settings: DatabaseSettings) {
    companion object {
        /**
         * [value] returns the raw value stored under `database.<key>`, or null when it is absent.
         * Matches Bukkit's getters: any scalar works as a string, numbers must be numbers, otherwise the default.
         */
        fun read(value: (String) -> Any?): DatabaseConfig {
            fun string(key: String, default: String) = value(key)?.toString() ?: default
            fun int(key: String, default: Int) = (value(key) as? Number)?.toInt() ?: default
            fun long(key: String, default: Long) = (value(key) as? Number)?.toLong() ?: default
            return DatabaseConfig(
                adapterId = string("adapter", "postgresql").lowercase(),
                settings = DatabaseSettings(
                    host = string("host", "localhost"),
                    port = int("port", 5432),
                    database = string("name", "minecraft"),
                    username = string("username", "minecraft"),
                    password = string("password", ""),
                    maximumPoolSize = int("maximum-pool-size", 10).coerceIn(1, 50),
                    connectionTimeoutMs = long("connection-timeout-ms", 5000).coerceIn(250, 30000),
                ),
            )
        }
    }
}

internal const val UNREADABLE_CONFIG = "SneakyDB configuration could not be read"

/**
 * Worker threads resolve classes through SneakyDB's own loader, so Hikari's context-loader lookup of the bundled
 * driver behaves the same under Paper's and Velocity's plugin classloaders.
 */
internal val workerThreads = ThreadFactory { task ->
    Thread(task, "SneakyDB-Worker").apply {
        isDaemon = true
        contextClassLoader = DatabaseSession::class.java.classLoader
    }
}

/** Starts one enable generation. A null [config] (unreadable file) fails readiness during adapter creation. */
internal fun startDatabaseSession(config: DatabaseConfig?, warn: (String) -> Unit): DatabaseSession {
    val executor = ThreadPoolExecutor(
        4, 4, 0L, TimeUnit.MILLISECONDS, ArrayBlockingQueue(256), workerThreads, ThreadPoolExecutor.AbortPolicy(),
    )
    val notifications = NotificationWorker(
        { openPostgreSqlNotificationConnection(checkNotNull(config) { UNREADABLE_CONFIG }.settings) },
        warn,
    )
    return DatabaseSession(executor, notifications) {
        checkNotNull(config) { UNREADABLE_CONFIG }
        require(config.adapterId == "postgresql") { "Unsupported database adapter" }
        PostgreSqlAdapter().createDataSource(config.settings)
    }
}

internal fun logReadinessFailure(logger: java.util.logging.Logger, failure: Throwable) =
    logReadinessFailure(logger::severe, failure)

/** Logs at most one credential-safe line through [severe]; a shutdown before readiness logs nothing. */
internal fun logReadinessFailure(severe: (String) -> Unit, failure: Throwable) {
    var diagnostic = failure
    while (diagnostic is java.util.concurrent.CompletionException && diagnostic.cause != null) {
        diagnostic = diagnostic.cause!!
    }
    val knownMessages = listOf("adapter creation", "connection acquisition", "connection validation").map {
        "SneakyDB initialization failed during $it; check adapter configuration and database access"
    }
    val message = diagnostic.message
    if (diagnostic is IllegalStateException && message in knownMessages) {
        severe(message!!)
    } else if (diagnostic !is IllegalStateException || message != "SneakyDB has shut down") {
        severe("SneakyDB initialization failed; check database configuration and access.")
    }
}
