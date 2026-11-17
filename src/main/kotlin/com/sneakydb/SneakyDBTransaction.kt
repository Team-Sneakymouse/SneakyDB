package com.sneakydb

import kotlin.jvm.JvmSuppressWildcards

/**
 * One open database transaction, handed to the work function of [SneakyDBApi.transaction]. Calls block and run on the
 * transaction's connection. The handle is valid only inside that work function, on the thread that runs it; any other
 * use throws IllegalStateException.
 *
 * A failed statement throws its SQLException (undeclared, so Java code sees it only as Exception). Let it propagate:
 * PostgreSQL aborts the whole transaction after a failed statement, and the work function throwing rolls it back.
 *
 * Keep signatures to JDK types only, like [SneakyDBApi]; consumers compile against this interface.
 */
interface SneakyDBTransaction {
    /** Executes a query in this transaction; rows have the same shape as [SneakyDBApi.query]. */
    @JvmSuppressWildcards
    fun query(sql: String, parameters: List<*>): List<Map<String, Any?>>

    /** Executes an insert, update, or delete in this transaction and returns the affected row count. */
    @JvmSuppressWildcards
    fun execute(sql: String, parameters: List<*>): Int
}
