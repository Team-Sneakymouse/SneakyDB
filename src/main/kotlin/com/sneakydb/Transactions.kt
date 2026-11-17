package com.sneakydb

import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection

// JDBC helpers shared by DatabaseSession.query/execute and transaction handles, so both give the same row shape.

internal fun readRows(connection: Connection, sql: String, parameters: List<*>): List<Map<String, Any?>> =
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

internal fun executeUpdate(connection: Connection, sql: String, parameters: List<*>): Int =
    connection.prepareStatement(sql).use { statement ->
        parameters.forEachIndexed { index, value -> statement.setObject(index + 1, value) }
        statement.executeUpdate()
    }

/**
 * One transaction in progress on a dedicated pooled [connection] (autocommit off). SneakyDB's own code may use the
 * connection directly (any statement, including `SET LOCAL` and parameterless scripts); consumers only ever see a
 * [TransactionHandle].
 */
internal class TransactionScope(val connection: Connection, private val abandoned: () -> Boolean) {
    /** Set by SneakyDB code that may have left session state it can't undo: the connection is evicted, never reused. */
    @Volatile var discard = false

    /** Throws once shutdown or completion of the transaction's future has abandoned it, so no further statement runs. */
    fun checkActive() = check(!abandoned()) { "SneakyDB transaction abandoned by shutdown or cancellation; it will be rolled back" }
}

/**
 * Runs [block] in one transaction on a connection from [pool]: commits when it returns, rolls back when it throws or
 * when [abandoned] reports true before the commit starts. Autocommit is restored only after a successful commit or
 * rollback of a block that didn't set [TransactionScope.discard]. Otherwise the connection is evicted (closing it makes PostgreSQL discard the open transaction), so the pool
 * never hands out a connection with a transaction open: restoring autocommit mid-transaction would commit it.
 */
internal fun <T> runTransaction(pool: HikariDataSource, abandoned: () -> Boolean, block: (TransactionScope) -> T): T {
    val connection = pool.connection
    var reusable = false
    try {
        connection.autoCommit = false
        val scope = TransactionScope(connection, abandoned)
        val value = try {
            block(scope)
        } catch (error: Throwable) {
            reusable = rollback(connection, error) && !scope.discard
            throw error
        }
        if (abandoned()) {
            val error = IllegalStateException("SneakyDB transaction abandoned by shutdown or cancellation before commit; rolled back")
            reusable = rollback(connection, error)
            throw error
        }
        try {
            connection.commit()
        } catch (error: Throwable) {
            reusable = rollback(connection, error)
            throw error
        }
        reusable = true
        return value
    } finally {
        if (reusable) reusable = runCatching { connection.autoCommit = true }.isSuccess
        if (!reusable) runCatching { pool.evictConnection(connection) }
        runCatching { connection.close() }
    }
}

/** Rolls back; on failure records it on [cause] and returns false, so the caller discards the connection. */
private fun rollback(connection: Connection, cause: Throwable): Boolean = try {
    connection.rollback()
    true
} catch (error: Throwable) {
    cause.addSuppressed(error)
    false
}

/** The consumer's view of one transaction: valid only inside its work function, on the worker thread that runs it. */
internal class TransactionHandle(private val scope: TransactionScope) : SneakyDBTransaction {
    private val owner = Thread.currentThread()
    @Volatile private var open = true

    override fun query(sql: String, parameters: List<*>): List<Map<String, Any?>> =
        active { readRows(scope.connection, sql, parameters) }

    override fun execute(sql: String, parameters: List<*>): Int =
        active { executeUpdate(scope.connection, sql, parameters) }

    fun close() { open = false }

    private inline fun <R> active(statement: () -> R): R {
        check(open && Thread.currentThread() === owner) {
            "SneakyDB transaction handle used after its work function returned or from another thread"
        }
        scope.checkActive()
        return statement()
    }
}
