package com.sneakydb

import com.sneakydb.adapter.DatabaseSettings
import com.sneakydb.adapter.PostgreSqlAdapter
import com.zaxxer.hikari.HikariDataSource
import org.bukkit.plugin.java.JavaPlugin
import java.sql.ResultSet
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

open class SneakyDB : JavaPlugin() {
    private lateinit var executor: ExecutorService
    @Volatile private var dataSource: HikariDataSource? = null
    @Volatile private var stopping = false
    private val api = DatabaseApi()

    override fun onEnable() {
        saveDefaultConfig()
        executor = Executors.newFixedThreadPool(4) { task ->
            Thread(task, "SneakyDB-Query").apply { isDaemon = true }
        }
        SneakyDBRegistry.api = api
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
        executor.execute {
            try {
                val adapter = when (adapterId) {
                    "postgresql" -> PostgreSqlAdapter()
                    else -> throw IllegalArgumentException("Unknown database adapter '$adapterId'")
                }
                val source = adapter.createDataSource(settings)
                if (stopping) source.close() else {
                    dataSource = source
                    logger.info("PostgreSQL adapter connected and ready.")
                }
            } catch (failure: Exception) {
                logger.severe("Unable to initialize PostgreSQL adapter (${failure.javaClass.simpleName}).")
            }
        }
    }

    override fun onDisable() {
        stopping = true
        SneakyDBRegistry.api = null
        if (::executor.isInitialized) {
            executor.shutdown()
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) executor.shutdownNow()
        }
        dataSource?.close()
    }

    private inner class DatabaseApi : SneakyDBApi {
        override fun isAvailable(): Boolean = dataSource?.isClosed == false

        override fun query(sql: String, parameters: List<*>): CompletableFuture<List<Map<String, Any?>>> =
            submit { source ->
                source.connection.use { connection ->
                    connection.prepareStatement(sql).use { statement ->
                        bind(statement, parameters)
                        statement.executeQuery().use { result -> result.toRows() }
                    }
                }
            }

        override fun execute(sql: String, parameters: List<*>): CompletableFuture<Int> =
            submit { source ->
                source.connection.use { connection ->
                    connection.prepareStatement(sql).use { statement ->
                        bind(statement, parameters)
                        statement.executeUpdate()
                    }
                }
            }

        private fun <T> submit(operation: (HikariDataSource) -> T): CompletableFuture<T> =
            CompletableFuture.supplyAsync({
                val source = dataSource ?: throw IllegalStateException("SneakyDB database adapter is not available")
                if (source.isClosed) throw IllegalStateException("SneakyDB database adapter is closed")
                operation(source)
            }, executor)
    }

    private fun bind(statement: java.sql.PreparedStatement, parameters: List<*>) {
        parameters.forEachIndexed { index, value -> statement.setObject(index + 1, value) }
    }

    private fun ResultSet.toRows(): List<Map<String, Any?>> {
        val metadata = metaData
        val rows = ArrayList<Map<String, Any?>>()
        while (next()) {
            val row = LinkedHashMap<String, Any?>(metadata.columnCount)
            for (column in 1..metadata.columnCount) {
                row[metadata.getColumnLabel(column)] = getObject(column)
            }
            rows.add(java.util.Collections.unmodifiableMap(row))
        }
        return java.util.Collections.unmodifiableList(rows)
    }
}
