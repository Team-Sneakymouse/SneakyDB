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

internal const val NOTIFICATION_SOCKET_TIMEOUT_SECONDS = 15

/** Opens a plain, unpooled connection for LISTEN; SneakyDB's notification thread owns it. */
internal fun openPostgreSqlNotificationConnection(settings: DatabaseSettings): com.sneakydb.NotificationConnection {
    val timeoutSeconds = ((settings.connectionTimeoutMs + 999) / 1000).coerceAtLeast(1).toInt()
    val source = org.postgresql.ds.PGSimpleDataSource().apply {
        serverNames = arrayOf(settings.host)
        portNumbers = intArrayOf(settings.port)
        databaseName = settings.database
        user = settings.username
        password = settings.password
        connectTimeout = timeoutSeconds
        loginTimeout = timeoutSeconds
        tcpKeepAlive = true
        // Bounds LISTEN/UNLISTEN and every other read, so a stalled server can't hold the notification thread past
        // reconnect or shutdown. getNotifications(timeout) sets its own shorter wait.
        socketTimeout = NOTIFICATION_SOCKET_TIMEOUT_SECONDS
        applicationName = "SneakyDB-Notifications"
    }
    return PostgreSqlNotificationConnection(source.connection)
}

internal class PostgreSqlNotificationConnection(private val connection: java.sql.Connection) : com.sneakydb.NotificationConnection {
    private val pg = connection.unwrap(org.postgresql.PGConnection::class.java)

    // Channel names are checked against CHANNEL_NAME before they get here.
    override fun listen(channel: String) { connection.createStatement().use { it.execute("LISTEN \"$channel\"") } }
    override fun unlisten(channel: String) { connection.createStatement().use { it.execute("UNLISTEN \"$channel\"") } }
    override fun poll(timeoutMs: Int): List<Pair<String, String>> =
        pg.getNotifications(timeoutMs)?.map { it.name to (it.parameter ?: "") } ?: emptyList()
    override fun check() { check(connection.isValid(5)) }
    internal fun networkTimeoutMs(): Int = connection.networkTimeout
    override fun close() = connection.close()
}
