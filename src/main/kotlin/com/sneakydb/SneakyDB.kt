package com.sneakydb

import com.sneakydb.adapter.DatabaseSettings
import com.sneakydb.adapter.PostgreSqlAdapter
import org.bukkit.plugin.java.JavaPlugin
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

internal fun logReadinessFailure(logger: java.util.logging.Logger, failure: Throwable) {
    var diagnostic = failure
    while (diagnostic is java.util.concurrent.CompletionException && diagnostic.cause != null) {
        diagnostic = diagnostic.cause!!
    }
    val knownMessages = listOf("adapter creation", "connection acquisition", "connection validation").map {
        "SneakyDB initialization failed during $it; check adapter configuration and database access"
    }
    val message = diagnostic.message
    if (diagnostic is IllegalStateException && message in knownMessages) {
        logger.severe(message)
    } else if (diagnostic !is IllegalStateException || message != "SneakyDB has shut down") {
        logger.severe("SneakyDB initialization failed; check database configuration and access.")
    }
}

open class SneakyDB : JavaPlugin() {
    private var session: DatabaseSession? = null

    override fun onEnable() {
        saveDefaultConfig()
        val settings = DatabaseSettings(
            host = config.getString("database.host", "localhost")!!,
            port = config.getInt("database.port", 5432),
            database = config.getString("database.name", "minecraft")!!,
            username = config.getString("database.username", "minecraft")!!,
            password = config.getString("database.password", "")!!,
            maximumPoolSize = config.getInt("database.maximum-pool-size", 10).coerceIn(1, 50),
            connectionTimeoutMs = config.getLong("database.connection-timeout-ms", 5000).coerceIn(250, 30000),
        )
        val adapterId = config.getString("database.adapter", "postgresql")!!.lowercase()
        val executor = ThreadPoolExecutor(
            4, 4, 0L, TimeUnit.MILLISECONDS, ArrayBlockingQueue(256),
            { task -> Thread(task, "SneakyDB-Worker").apply { isDaemon = true } },
            ThreadPoolExecutor.AbortPolicy(),
        )
        val generation = DatabaseSession(executor) {
            require(adapterId == "postgresql") { "Unsupported database adapter" }
            PostgreSqlAdapter().createDataSource(settings)
        }
        session = generation
        SneakyDBRegistry.api = generation
        generation.whenReady().whenComplete { _, failure ->
            if (failure != null) logReadinessFailure(logger, failure)
            else if (generation.isAvailable()) logger.info("PostgreSQL adapter connected and ready.")
        }
    }

    override fun onDisable() {
        SneakyDBRegistry.api = null
        session?.close()
        session = null
    }
}
