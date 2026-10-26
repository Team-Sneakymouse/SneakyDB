package com.sneakydb.adapter

import com.zaxxer.hikari.HikariDataSource

/** Adapter owns vendor-specific connection setup; the consumer API stays database-neutral. */
interface DatabaseAdapter {
    val id: String
    fun createDataSource(settings: DatabaseSettings): HikariDataSource
}

data class DatabaseSettings(
    val host: String,
    val port: Int,
    val database: String,
    val username: String,
    val password: String,
    val maximumPoolSize: Int,
    val connectionTimeoutMs: Long,
)

class PostgreSqlAdapter : DatabaseAdapter {
    override val id = "postgresql"

    override fun createDataSource(settings: DatabaseSettings) = HikariDataSource().apply {
        poolName = "SneakyDB-PostgreSQL"
        driverClassName = "org.postgresql.Driver"
        jdbcUrl = "jdbc:postgresql://${settings.host}:${settings.port}/${settings.database}"
        username = settings.username
        password = settings.password
        maximumPoolSize = settings.maximumPoolSize
        connectionTimeout = settings.connectionTimeoutMs
        initializationFailTimeout = settings.connectionTimeoutMs
        // Bound the driver's initial network/login attempt as well as Hikari acquisition.
        val timeoutSeconds = ((settings.connectionTimeoutMs + 999) / 1000).coerceAtLeast(1).toString()
        addDataSourceProperty("connectTimeout", timeoutSeconds)
        addDataSourceProperty("loginTimeout", timeoutSeconds)
    }
}
