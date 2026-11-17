package com.sneakydb

import java.sql.Connection
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException

/** Advisory lock key ("SneakyDB" in ASCII) that serialises migration work of every server on one database. */
internal const val MIGRATION_LOCK_KEY = 0x536E65616B794442L

private const val SCRIPT_SAVEPOINT = "sneakydb_script"
private const val CHECK_BY_HAND =
    "check them by hand, remove the transaction control from the script, and undo or complete the partial changes before restarting"

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
        // The check for transaction control in scripts reads strings as PostgreSQL does by default; with this off,
        // PostgreSQL would read '\' as an escaped quote and could run a COMMIT the check took for part of a string.
        val conforming = single(connection, "select current_setting('standard_conforming_strings')") { it.getString(1) }
        if (conforming != "on") throw MigrationException(
            "standard_conforming_strings is $conforming on the connection that would run ${script.fileName}; migrations need " +
                "PostgreSQL's default string parsing, so the script did not run. Set it back to on for the database and role " +
                "(alter database … reset standard_conforming_strings; alter role … reset standard_conforming_strings) and restart the server")
        connection.prepareStatement("select set_config('lock_timeout', ?, true)").use {
            it.setString(1, "${lockTimeoutSeconds}s")
            it.executeQuery().close()
        }
        scope.checkActive()
        val transaction = currentTransaction(connection)
        // Lets a failed script's transaction be proven unchanged: only the transaction the savepoint was set in has it.
        execute(connection, "savepoint $SCRIPT_SAVEPOINT")
        try {
            connection.createStatement().use { statement ->
                statement.setEscapeProcessing(false) // run the file exactly as written, without JDBC {escape} rewriting
                statement.execute(script.text)
            }
        } catch (error: java.sql.SQLException) {
            // A transaction that still answers is not aborted. After a server error that means the script ended its own
            // transaction (pgjdbc began a new one for the probe), unless the error never reached the server.
            val unaborted = runCatching { currentTransaction(connection) }.getOrNull()
            val same = if (unaborted != null) unaborted == transaction else {
                // Normally the failed script left its own transaction aborted: only that one has the savepoint.
                runCatching { execute(connection, "rollback to savepoint $SCRIPT_SAVEPOINT") }.isSuccess &&
                    runCatching { currentTransaction(connection) }.getOrNull() == transaction
            }
            if (same) throw error
            // Another transaction, or one that can't be identified (aborted after COMMIT; BEGIN, or a lost connection).
            scope.discard = true
            throw MigrationException(
                if (unaborted != null) "${endedTransaction(script)}; then it failed: ${describeFailure(error)}"
                else "${script.fileName} failed (${describeFailure(error)}), and SneakyDB could not confirm that it was still in the " +
                    "transaction SneakyDB runs it in (a COMMIT or ROLLBACK in the script followed by BEGIN has this effect), so its " +
                    "version was not recorded and the database may hold part of its changes; $CHECK_BY_HAND")
        }
        if (currentTransaction(connection) != transaction) {
            // Session settings the script set before ending its transaction would survive; don't reuse the connection.
            scope.discard = true
            throw MigrationException(endedTransaction(script))
        }
        execute(connection, "release savepoint $SCRIPT_SAVEPOINT")
        scope.checkActive()
        // Session-level SET in a script would otherwise outlive it on this pooled connection.
        execute(connection, "reset all")
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
            "recorded and the database may hold part of its changes; $CHECK_BY_HAND"

    private fun execute(connection: Connection, sql: String) = connection.createStatement().use { it.execute(sql) }

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
