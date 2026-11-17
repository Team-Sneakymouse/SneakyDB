package com.sneakydb

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.sql.SQLException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.TimeUnit

/** Commit, rollback and handle rules of [DatabaseSession.transaction]; lifecycle cases are in DatabaseSessionTest. */
class TransactionTest {
    private val worker = DatabaseSessionTest.Worker()
    private val pool = DatabaseSessionTest.Pool()
    private val session = DatabaseSession(worker) { pool }.also {
        worker.runNext()
        pool.events.clear()
    }

    @AfterEach fun close() = session.close()

    private fun failure(future: CompletableFuture<*>): Throwable =
        assertThrows(CompletionException::class.java) { future.join() }.cause!!

    @Test fun `work that returns commits and completes with its value`() {
        lateinit var rows: List<Map<String, Any?>>
        val result = session.transaction { tx ->
            rows = tx.query("SELECT ? AS label", listOf("x"))
            tx.execute("UPDATE a SET x = ?", listOf(42))
        }
        assertFalse(result.isDone, "work runs on a worker, not the caller")
        worker.runNext()
        assertEquals(7, result.join())
        assertEquals(listOf(mapOf("label" to "value")), rows)
        assertThrows(UnsupportedOperationException::class.java) { (rows as MutableList<Map<String, Any?>>).clear() }
        assertEquals(mapOf(1 to 42), pool.bindings.filterKeys { it == 1 })
        assertEquals(listOf("autoCommit=false", "sql:SELECT ? AS label", "sql:UPDATE a SET x = ?", "commit", "autoCommit=true", "close"), pool.events)
        assertEquals(2, pool.acquired, "one connection for initialization, one for the whole transaction")
    }

    @Test fun `work that throws rolls back and fails the future with that exception`() {
        val thrown = IllegalArgumentException("consumer failure")
        val result = session.transaction<Int> { tx ->
            tx.execute("DELETE FROM a", emptyList<Any>())
            throw thrown
        }
        worker.runNext()
        assertSame(thrown, failure(result))
        assertEquals(listOf("autoCommit=false", "sql:DELETE FROM a", "rollback", "autoCommit=true", "close"), pool.events)
        assertFalse(pool.evicted)
    }

    @Test fun `a failed statement propagates and rolls back`() {
        val statement = SQLException("duplicate key", "23505")
        pool.statementFailure = statement
        val result = session.transaction { tx -> tx.execute("INSERT INTO a VALUES (1)", emptyList<Any>()) }
        worker.runNext()
        assertSame(statement, failure(result))
        assertEquals(listOf("autoCommit=false", "rollback", "autoCommit=true", "close"), pool.events)
    }

    @Test fun `an Error from work still settles the future and rolls back`() {
        val result = session.transaction<Int> { throw AssertionError("consumer assertion") }
        worker.runNext()
        assertTrue(failure(result) is AssertionError)
        assertEquals(listOf("autoCommit=false", "rollback", "autoCommit=true", "close"), pool.events)
    }

    @Test fun `a failed commit fails the future`() {
        val commit = SQLException("could not serialize access", "40001")
        pool.commitFailure = commit
        val result = session.transaction { tx -> tx.execute("UPDATE a SET x = 1", emptyList<Any>()) }
        worker.runNext()
        assertSame(commit, failure(result))
        assertEquals(listOf("autoCommit=false", "sql:UPDATE a SET x = 1", "rollback", "autoCommit=true", "close"), pool.events)
    }

    @Test fun `a failed rollback discards the connection instead of restoring autocommit`() {
        val thrown = IllegalStateException("consumer failure")
        val rollback = SQLException("connection reset", "08006")
        pool.rollbackFailure = rollback
        val result = session.transaction<Int> { tx ->
            tx.execute("UPDATE a SET x = 1", emptyList<Any>())
            throw thrown
        }
        worker.runNext()
        assertSame(thrown, failure(result))
        assertSame(rollback, thrown.suppressed.single())
        // Restoring autocommit with a transaction open would commit it.
        assertEquals(listOf("autoCommit=false", "sql:UPDATE a SET x = 1", "evict", "close"), pool.events)
    }

    @Test fun `a failed commit and rollback discards the connection`() {
        pool.commitFailure = SQLException("connection reset", "08006")
        pool.rollbackFailure = SQLException("connection reset", "08006")
        val result = session.transaction { 1 }
        worker.runNext()
        assertSame(pool.commitFailure, failure(result))
        assertEquals(listOf("autoCommit=false", "evict", "close"), pool.events)
    }

    @Test fun `the handle refuses use after work returns`() {
        var escaped: SneakyDBTransaction? = null
        val result = session.transaction { tx -> escaped = tx; 1 }
        worker.runNext()
        assertEquals(1, result.join())
        assertThrows(IllegalStateException::class.java) { escaped!!.query("SELECT 1", emptyList<Any>()) }
        assertThrows(IllegalStateException::class.java) { escaped!!.execute("DELETE FROM a", emptyList<Any>()) }
        assertFalse(pool.events.any { it.startsWith("sql:") })
    }

    @Test fun `the handle refuses use from another thread`() {
        val result = session.transaction { tx ->
            CompletableFuture.supplyAsync { runCatching { tx.execute("DELETE FROM a", emptyList<Any>()) }.exceptionOrNull() }
                .get(2, TimeUnit.SECONDS)
        }
        worker.runNext()
        assertTrue(result.join() is IllegalStateException)
        assertFalse(pool.events.any { it.startsWith("sql:") })
        assertTrue("commit" in pool.events, "the refused call does not by itself fail the transaction")
    }

    @Test fun `SneakyDB calls inside work are rejected without taking a worker or connection`() {
        val result = session.transaction {
            listOf(session.query("SELECT 1", emptyList<Any>()), session.execute("DELETE FROM a", emptyList<Any>()), session.transaction { 1 })
        }
        worker.runNext()
        for (nested in result.join()) assertTrue(failure(nested) is IllegalStateException)
        assertTrue(worker.tasks.isEmpty())
        assertEquals(2, pool.acquired)

        // The marker is cleared afterwards: the same thread may use the API again (the fake worker runs on this thread).
        val later = session.query("SELECT 1", emptyList<Any>())
        worker.runNext()
        assertEquals(1, later.join().size)
    }

    @Test fun `joining a SneakyDB call inside work fails fast and rolls back`() {
        val result = session.transaction { session.query("SELECT 1", emptyList<Any>()).join() }
        worker.runNext()
        val error = failure(result)
        assertTrue(error is IllegalStateException && "transaction handle" in error.message!!, error.toString())
        assertEquals(listOf("autoCommit=false", "rollback", "autoCommit=true", "close"), pool.events)
    }

    @Test fun `completing the future during work rolls back instead of committing`() {
        lateinit var result: CompletableFuture<Int>
        result = session.transaction { tx ->
            tx.execute("UPDATE a SET x = 1", emptyList<Any>())
            result.cancel(false)
            assertThrows(IllegalStateException::class.java) { tx.execute("UPDATE b SET x = 1", emptyList<Any>()) }
            1
        }
        worker.runNext()
        assertTrue(result.isCancelled)
        assertEquals(listOf("autoCommit=false", "sql:UPDATE a SET x = 1", "rollback", "autoCommit=true", "close"), pool.events)
    }

    @Test fun `cancelling before a worker starts acquires no connection`() {
        val result = session.transaction { 1 }
        result.cancel(false)
        worker.runNext()
        assertEquals(1, pool.acquired)
        assertTrue(pool.events.isEmpty())
    }

    @Test fun `implementations without transaction support keep compiling and refuse`() {
        val api = object : SneakyDBApi {
            override fun query(sql: String, parameters: List<*>) = CompletableFuture<List<Map<String, Any?>>>()
            override fun execute(sql: String, parameters: List<*>) = CompletableFuture<Int>()
            override fun whenReady() = CompletableFuture<Void>()
            override fun isAvailable() = false
        }
        assertThrows(UnsupportedOperationException::class.java) { api.transaction { 1 } }
    }
}
