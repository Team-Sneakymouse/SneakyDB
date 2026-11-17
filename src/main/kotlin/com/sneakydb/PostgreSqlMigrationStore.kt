package com.sneakydb

import java.sql.Connection
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException

/** Advisory lock key ("SneakyDB" in ASCII) that serialises migration work of every server on one database. */
internal const val MIGRATION_LOCK_KEY = 0x536E65616B794442L

private const val CREATE_TABLE = """
create table if not exists sneakydb.schema_migrations (
    plugin     text        not null,
    version    integer     not null,
    name       text        not null,
    checksum   text,
    applied_at timestamptz not null default now(),
    primary key (plugin, version)
)"""

/**
 * The version table `sneakydb.schema_migrations` on PostgreSQL. Each call is one transaction through
 * [DatabaseSession.inTransaction]; writes take a transaction-level advisory lock first, so servers sharing the
 * database apply scripts one at a time and each sees what the others committed.
 */
internal class PostgreSqlMigrationStore(private val session: DatabaseSession) : MigrationStore {
    override fun readApplied(mode: MigrationMode): Map<String, List<AppliedVersion>> = await(session.inTransaction { scope ->
        val connection = scope.connection
        val role = single(connection, "select quote_ident(current_user)") { it.getString(1) }
        val schemaExists = single(connection, "select exists (select 1 from pg_namespace where nspname = 'sneakydb')") { it.getBoolean(1) }
        if (!schemaExists) throw MigrationException(
            "the schema sneakydb does not exist; as the database owner run: create schema sneakydb; " +
                "grant usage, create on schema sneakydb to $role; then restart the server")
        val (usage, create) = single(connection,
            "select has_schema_privilege('sneakydb', 'USAGE'), has_schema_privilege('sneakydb', 'CREATE')") { it.getBoolean(1) to it.getBoolean(2) }
        if (!usage) throw MigrationException(
            "role $role may not use the schema sneakydb; as the database owner run: grant usage, create on schema sneakydb to $role; then restart the server")
        val tableExists = single(connection, "select to_regclass('sneakydb.schema_migrations') is not null") { it.getBoolean(1) }
        if (!tableExists) {
            if (mode == MigrationMode.VERIFY) return@inTransaction emptyMap()
            if (!create) throw MigrationException(
                "sneakydb.schema_migrations is missing and role $role may not create it; as the database owner run: " +
                    "grant usage, create on schema sneakydb to $role; then restart the server")
            lock(connection)
            connection.createStatement().use { it.execute(CREATE_TABLE) }
        }
        readAll(connection)
    })

    override fun fillChecksums(plugin: String, scripts: List<MigrationScript>): List<Int> = await(session.inTransaction { scope ->
        val connection = scope.connection
        lock(connection)
        // Another server may have filled them meanwhile, from a different script: recheckFill compares those.
        val versions = recheckFill(plugin, scripts, readAll(connection))
        connection.prepareStatement(
            "update sneakydb.schema_migrations set checksum = ? where plugin = ? and version = ? and checksum is null",
        ).use { statement ->
            for (version in versions) {
                statement.setString(1, scripts[version - 1].checksum)
                statement.setString(2, plugin)
                statement.setInt(3, version)
                statement.executeUpdate()
            }
        }
        versions
    })

    override fun confirmReady(plugin: String, scripts: List<MigrationScript>) = await(session.inTransaction { scope ->
        val connection = scope.connection
        lock(connection)
        val tableExists = single(connection, "select to_regclass('sneakydb.schema_migrations') is not null") { it.getBoolean(1) }
        recheckReady(plugin, scripts, if (tableExists) readAll(connection) else emptyMap())
    })

    override fun apply(step: PlannedStep, lockTimeoutSeconds: Int): Boolean = await(session.inTransaction { scope ->
        val connection = scope.connection
        val script = step.script
        lock(connection)
        // Re-check under the lock: another server may have applied this or other scripts since the plan was made.
        if (!recheckStep(step, readAll(connection))) return@inTransaction false
        connection.prepareStatement("select set_config('lock_timeout', ?, true)").use {
            it.setString(1, "${lockTimeoutSeconds}s")
            it.executeQuery().close()
        }
        scope.checkActive()
        val transaction = currentTransaction(connection)
        try {
            connection.createStatement().use { statement ->
                statement.setEscapeProcessing(false) // run the file exactly as written, without JDBC {escape} rewriting
                statement.execute(script.text)
            }
        } catch (error: java.sql.SQLException) {
            // Normally the failed script leaves its own transaction aborted, and the probe fails with 25P02.
            if (runCatching { currentTransaction(connection) != transaction }.getOrDefault(false)) {
                scope.discard = true
                throw MigrationException("${endedTransaction(script)}; then it failed: ${describeFailure(error)}")
            }
            throw error
        }
        if (currentTransaction(connection) != transaction) {
            // Session settings the script set before ending its transaction would survive; don't reuse the connection.
            scope.discard = true
            throw MigrationException(endedTransaction(script))
        }
        scope.checkActive()
        // Session-level SET in a script would otherwise outlive it on this pooled connection.
        connection.createStatement().use { it.execute("reset all") }
        connection.prepareStatement(
            "insert into sneakydb.schema_migrations (plugin, version, name, checksum) values (?, ?, ?, ?)",
        ).use { statement ->
            statement.setString(1, step.plugin)
            statement.setInt(2, script.version)
            statement.setString(3, script.fileName)
            statement.setString(4, script.checksum)
            statement.executeUpdate()
        }
        true
    })

    private fun endedTransaction(script: MigrationScript) =
        "${script.fileName} ended the transaction SneakyDB runs it in (COMMIT, ROLLBACK or similar), so its version was not " +
            "recorded and the database may hold part of its changes; check them by hand, remove the transaction control from " +
            "the script, and undo or complete the partial changes before restarting"

    /** The id of the transaction [connection] is in; a script that ends its transaction makes the next call return another. */
    private fun currentTransaction(connection: Connection): Long = single(connection, "select txid_current()") { it.getLong(1) }

    /** Every row of the version table, by plugin. */
    private fun readAll(connection: Connection): Map<String, List<AppliedVersion>> =
        connection.prepareStatement("select plugin, version, checksum from sneakydb.schema_migrations").use { statement ->
            statement.executeQuery().use { rows ->
                val result = HashMap<String, MutableList<AppliedVersion>>()
                while (rows.next()) result.getOrPut(rows.getString(1)) { mutableListOf() }.add(AppliedVersion(rows.getInt(2), rows.getString(3)))
                result
            }
        }

    /** Waits for the next migration lock of this database; released when the transaction ends. Not bound by lock_timeout. */
    private fun lock(connection: Connection) {
        connection.prepareStatement("select pg_advisory_xact_lock(?)").use {
            it.setLong(1, MIGRATION_LOCK_KEY)
            it.executeQuery().close()
        }
    }

    private fun <T> single(connection: Connection, sql: String, vararg parameters: String, read: (java.sql.ResultSet) -> T): T =
        connection.prepareStatement(sql).use { statement ->
            parameters.forEachIndexed { index, value -> statement.setString(index + 1, value) }
            statement.executeQuery().use { check(it.next()); read(it) }
        }

    /** Blocks the migration thread for [future] and rethrows its own failure, not a CompletionException around it. */
    private fun <T> await(future: CompletableFuture<T>): T = try {
        future.join()
    } catch (error: CompletionException) {
        throw error.cause ?: error
    }
}
