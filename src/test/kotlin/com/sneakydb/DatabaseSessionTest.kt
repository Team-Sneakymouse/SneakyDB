package com.sneakydb

import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.lang.reflect.Proxy
import java.sql.Connection
import java.util.concurrent.AbstractExecutorService
import java.util.concurrent.TimeUnit

class DatabaseSessionTest {
    class Worker : AbstractExecutorService() {
        val tasks = ArrayDeque<Runnable>()
        var stopped = false
        override fun execute(task: Runnable) {
            if (stopped) throw java.util.concurrent.RejectedExecutionException()
            tasks.add(task)
        }
        fun runNext() = tasks.removeFirst().run()
        override fun shutdown() { stopped = true }
        override fun shutdownNow(): MutableList<Runnable> { stopped = true; return tasks.toMutableList().also { tasks.clear() } }
        override fun isShutdown() = stopped
        override fun isTerminated() = stopped && tasks.isEmpty()
        override fun awaitTermination(timeout: Long, unit: TimeUnit) = isTerminated
    }
    class Pool : HikariDataSource() {
        var acquired = 0
        var closed = false
        var valid = true
        var timeout = 0
        var failure: RuntimeException? = null
        var onValidate: () -> Unit = {}
        var connectionClosed = false
        var preparedSql: String? = null
        val bindings = linkedMapOf<Int, Any?>()
        /** Transaction-relevant connection calls in order, e.g. `autoCommit=false`, `sql:...`, `commit`, `close`. */
        val events = java.util.Collections.synchronizedList(mutableListOf<String>())
        var commitFailure: java.sql.SQLException? = null
        var rollbackFailure: java.sql.SQLException? = null
        var statementFailure: java.sql.SQLException? = null
        var evicted = false
        override fun evictConnection(connection: Connection) { evicted = true; events.add("evict") }
        override fun getConnection(): Connection {
            acquired++
            failure?.let { throw it }
            return Proxy.newProxyInstance(javaClass.classLoader, arrayOf(Connection::class.java)) { _, method, args ->
                when (method.name) {
                    "isValid" -> { timeout = args!![0] as Int; onValidate(); valid }
                    "close" -> { connectionClosed = true; events.add("close"); null }
                    "setAutoCommit" -> { events.add("autoCommit=${args!![0]}"); null }
                    "commit" -> { commitFailure?.let { throw it }; events.add("commit"); null }
                    "rollback" -> { rollbackFailure?.let { throw it }; events.add("rollback"); null }
                    "prepareStatement" -> {
                        statementFailure?.let { throw it }
                        preparedSql = args!![0] as String
                        events.add("sql:$preparedSql")
                        Proxy.newProxyInstance(javaClass.classLoader, arrayOf(java.sql.PreparedStatement::class.java)) { _, call, values ->
                            when (call.name) {
                                "setObject" -> { bindings[values!![0] as Int] = values[1]; null }
                                "executeUpdate" -> 7
                                "executeQuery" -> resultSet()
                                "close" -> null
                                else -> throw UnsupportedOperationException(call.name)
                            }
                        }
                    }
                    else -> throw UnsupportedOperationException(method.name)
                }
            } as Connection
        }
        private fun resultSet(): java.sql.ResultSet {
            var next = true
            val metadata = Proxy.newProxyInstance(javaClass.classLoader, arrayOf(java.sql.ResultSetMetaData::class.java)) { _, method, _ ->
                when (method.name) { "getColumnCount" -> 1; "getColumnLabel" -> "label"; else -> error(method.name) }
            }
            return Proxy.newProxyInstance(javaClass.classLoader, arrayOf(java.sql.ResultSet::class.java)) { _, method, _ ->
                when (method.name) {
                    "next" -> next.also { next = false }
                    "getMetaData" -> metadata
                    "getObject" -> "value"
                    "close" -> null
                    else -> error(method.name)
                }
            } as java.sql.ResultSet
        }
        var closeFailure = false
        override fun close() { closed = true; if (closeFailure) error("secret close failure") }
    }

    @Test fun `PostgreSQL adapter configures finite driver connection waits without connecting`() {
        val settings = com.sneakydb.adapter.DatabaseSettings("localhost", 5432, "test", "user", "secret", 2, 1250)
        com.sneakydb.adapter.PostgreSqlAdapter().createDataSource(settings).use { pool ->
            assertEquals("2", pool.dataSourceProperties.getProperty("connectTimeout"))
            assertEquals("2", pool.dataSourceProperties.getProperty("loginTimeout"))
            assertEquals(1250, pool.connectionTimeout)
            assertFalse(pool.isRunning)
        }
    }

    @Test fun `subscriber callbacks execute outside lifecycle lock`() {
        val worker = Worker()
        val session = DatabaseSession(worker) { Pool() }
        val callback = session.whenReady().thenRun {
            java.util.concurrent.CompletableFuture.runAsync { session.close() }.get(2, TimeUnit.SECONDS)
        }
        worker.runNext()
        callback.join()
        assertFalse(session.isAvailable())
    }

    @Test fun `validation exception closes the acquired connection and pool`() {
        val worker = Worker()
        val pool = Pool().apply { onValidate = { throw java.sql.SQLException("private credentials") } }
        val session = DatabaseSession(worker) { pool }
        worker.runNext()
        assertTrue(session.whenReady().isCompletedExceptionally)
        assertTrue(pool.connectionClosed)
        assertTrue(pool.closed)
        session.close()
    }

    @Test fun `adapter construction failure settles current and subsequent subscribers`() {
        val worker = Worker()
        val session = DatabaseSession(worker) { throw IllegalArgumentException("unsupported private adapter") }
        val waiting = session.whenReady()
        worker.runNext()
        assertTrue(waiting.isCompletedExceptionally)
        assertTrue(session.whenReady().isCompletedExceptionally)
        session.close()
    }

    @Test fun `shutdown before initialization starts settles readiness without creating a pool`() {
        val worker = Worker()
        var created = false
        val session = DatabaseSession(worker) { created = true; Pool() }
        val waiting = session.whenReady()
        session.close()
        assertTrue(waiting.isCompletedExceptionally)
        assertFalse(created)
        assertTrue(worker.tasks.isEmpty())
    }

    @Test fun `real executor performs acquisition and validation away from caller thread`() {
        val caller = Thread.currentThread()
        val worker = java.util.concurrent.Executors.newSingleThreadExecutor()
        val pool = Pool().apply { onValidate = { assertNotSame(caller, Thread.currentThread()) } }
        val session = DatabaseSession(worker) { assertNotSame(caller, Thread.currentThread()); pool }
        try { session.whenReady().get(2, TimeUnit.SECONDS) } finally { session.close() }
    }

    @Test fun `executor rejection is returned as an exceptional future`() {
        val worker = Worker()
        val session = DatabaseSession(worker) { Pool() }
        worker.runNext()
        worker.shutdown()
        assertTrue(session.query("ignored", emptyList<Any>()).isCompletedExceptionally)
        assertTrue(session.execute("ignored", emptyList<Any>()).isCompletedExceptionally)
        session.close()
    }

    @Test fun `cleanup failure cannot leave initialization subscribers pending`() {
        val worker = Worker()
        val pool = Pool().apply { valid = false; closeFailure = true }
        val session = DatabaseSession(worker) { pool }
        runCatching { worker.runNext() }
        assertTrue(session.whenReady().isCompletedExceptionally)
        session.close()
    }

    @Test fun `query and execute fail fast while pending and preserve SQL and parameter binding after readiness`() {
        val worker = Worker()
        val pool = Pool()
        val session = DatabaseSession(worker) { pool }
        assertTrue(session.query("pending", emptyList<Any>()).isCompletedExceptionally)
        assertTrue(session.execute("pending", emptyList<Any>()).isCompletedExceptionally)
        assertEquals(1, worker.tasks.size)
        worker.runNext()
        val sql = "SELECT ? AS label /* keep dialect */"
        val query = session.query(sql, listOf("x'; DROP TABLE example; --", null))
        worker.runNext()
        assertEquals(listOf(mapOf("label" to "value")), query.join())
        assertEquals(sql, pool.preparedSql)
        assertEquals(mapOf(1 to "x'; DROP TABLE example; --", 2 to null), pool.bindings)
        pool.bindings.clear()
        val update = session.execute("UPDATE example SET x = ?", listOf(42))
        worker.runNext()
        assertEquals(7, update.join())
        assertEquals("UPDATE example SET x = ?", pool.preparedSql)
        assertEquals(mapOf(1 to 42), pool.bindings)
        session.close()
    }

    @Test fun `acquisition failure is terminal credential safe and does not retry`() {
        val worker = Worker()
        val pool = Pool().apply { failure = IllegalStateException("password=super-secret jdbc:postgresql://private") }
        val session = DatabaseSession(worker) { pool }
        val before = session.whenReady()
        worker.runNext()
        assertTrue(before.isCompletedExceptionally)
        val error = assertThrows(java.util.concurrent.CompletionException::class.java) { session.whenReady().join() }
        assertTrue(error.cause is IllegalStateException)
        assertTrue(error.cause!!.message!!.contains("initialization failed"))
        assertTrue(error.cause!!.message!!.contains("connection acquisition"))
        assertFalse(error.stackTraceToString().contains("super-secret"))
        assertFalse(session.isAvailable())
        assertTrue(pool.closed)
        assertTrue(session.query("ignored", emptyList<Any>()).isCompletedExceptionally)
        assertTrue(session.execute("ignored", emptyList<Any>()).isCompletedExceptionally)
        assertEquals(1, pool.acquired)
        assertTrue(worker.tasks.isEmpty())
        session.close()
    }

    @Test fun `invalid connection is a terminal initialization failure`() {
        val worker = Worker()
        val pool = Pool().apply { valid = false }
        val session = DatabaseSession(worker) { pool }
        worker.runNext()
        assertTrue(session.whenReady().isCompletedExceptionally)
        assertTrue(pool.closed)
        assertTrue(pool.connectionClosed)
        assertFalse(session.isAvailable())
        session.close()
    }

    @Test fun `pending shutdown rejects readiness and late initialization closes its pool`() {
        val worker = Worker()
        val pool = Pool()
        lateinit var session: DatabaseSession
        session = DatabaseSession(worker) { pool }
        pool.onValidate = { session.close() }
        val before = session.whenReady()
        worker.runNext()
        assertTrue(before.isCompletedExceptionally)
        assertTrue(session.whenReady().isCompletedExceptionally)
        assertFalse(session.isAvailable())
        assertTrue(pool.closed)
        assertTrue(pool.connectionClosed)
    }

    @Test fun `old API remains closed when a new enable generation becomes ready`() {
        val oldWorker = Worker()
        val old = DatabaseSession(oldWorker) { Pool() }
        oldWorker.runNext()
        old.close()
        val worker = Worker()
        val current = DatabaseSession(worker) { Pool() }
        worker.runNext()
        assertTrue(current.isAvailable())
        assertFalse(old.isAvailable())
        assertTrue(old.execute("ignored", emptyList<Any>()).isCompletedExceptionally)
        assertTrue(old.query("ignored", emptyList<Any>()).isCompletedExceptionally)
        // Readiness is historical, not a live health/lifecycle notification.
        old.whenReady().join()
        current.close()
    }

    @Test fun `shutdown callbacks cannot admit other queued operations`() {
        val worker = Worker()
        val pool = Pool()
        val session = DatabaseSession(worker) { pool }
        worker.runNext()
        val first = session.execute("DELETE FROM example", emptyList<Any>())
        val second = session.query("SELECT * FROM example", emptyList<Any>())
        val drain = { while (worker.tasks.isNotEmpty()) worker.runNext() }
        first.whenComplete { _, _ -> drain() }
        second.whenComplete { _, _ -> drain() }
        session.close()
        assertEquals(1, pool.acquired, "Shutdown must prevent queued operations acquiring connections")
        assertTrue(first.isCompletedExceptionally)
        assertTrue(second.isCompletedExceptionally)
    }

    @Test fun `shutdown settles queued operations without running database work`() {
        val worker = Worker()
        val session = DatabaseSession(worker) { Pool() }
        worker.runNext()
        val queued = session.execute("DELETE FROM example", emptyList<Any>())
        session.close()
        assertTrue(queued.isCompletedExceptionally)
        assertTrue(session.execute("ignored", emptyList<Any>()).isCompletedExceptionally)
        assertFalse(session.isAvailable())
    }

    @Test fun `lazy pool is not ready until connection validated and subscribers cannot poison readiness`() {
        val worker = Worker()
        val pool = Pool()
        val session = DatabaseSession(worker) { pool }
        val first = session.whenReady()
        first.complete(null)
        val waiting = session.whenReady()
        assertFalse(waiting.isDone)
        assertFalse(session.isAvailable())
        assertEquals(0, pool.acquired)
        worker.runNext()
        waiting.join()
        session.whenReady().join()
        assertTrue(session.isAvailable())
        assertTrue(pool.timeout in 1..10)
        assertTrue(pool.connectionClosed)
    }

    @Test fun `transaction follows query admission rules and never waits for readiness`() {
        val worker = Worker()
        val pool = Pool()
        val session = DatabaseSession(worker) { pool }
        assertTrue(session.transaction { 1 }.isCompletedExceptionally, "pending")
        assertEquals(1, worker.tasks.size, "only initialization is queued")
        worker.runNext()
        worker.shutdown()
        val rejected = session.transaction { 1 }
        val error = assertThrows(java.util.concurrent.CompletionException::class.java) { rejected.join() }
        assertTrue(error.cause!!.message!!.contains("queue capacity exhausted"))
        session.close()
        assertTrue(session.transaction { 1 }.isCompletedExceptionally, "after shutdown")
        assertEquals(1, pool.acquired)

        val failing = Worker()
        val failed = DatabaseSession(failing) { error("password=secret") }
        failing.runNext()
        val initialization = assertThrows(java.util.concurrent.CompletionException::class.java) { failed.transaction { 1 }.join() }
        assertTrue(initialization.cause!!.message!!.contains("initialization failed"))
        assertFalse(initialization.stackTraceToString().contains("secret"))
        failed.close()
    }

    @Test fun `shutdown during a transaction fails its future, refuses further statements and rolls back`() {
        val worker = Worker()
        val pool = Pool()
        val session = DatabaseSession(worker) { pool }
        worker.runNext()
        pool.events.clear()
        val result = session.transaction { tx ->
            tx.execute("UPDATE a SET x = 1", emptyList<Any>())
            session.close()
            tx.execute("UPDATE b SET x = 1", emptyList<Any>())
        }
        worker.runNext()
        val error = assertThrows(java.util.concurrent.CompletionException::class.java) { result.join() }
        assertEquals("SneakyDB has shut down", error.cause!!.message)
        assertEquals(listOf("autoCommit=false", "sql:UPDATE a SET x = 1", "rollback", "autoCommit=true", "close"), pool.events)

        // Work that returns after shutdown without another statement is rolled back too, never committed.
        val quiet = Worker()
        val quietPool = Pool()
        val other = DatabaseSession(quiet) { quietPool }
        quiet.runNext()
        quietPool.events.clear()
        other.transaction { tx -> tx.execute("UPDATE a SET x = 1", emptyList<Any>()); other.close(); 1 }
        quiet.runNext()
        assertFalse("commit" in quietPool.events)
        assertTrue("rollback" in quietPool.events)
    }

    @Test fun `shutdown does not wait for a blocked transaction`() {
        val pool = Pool()
        val session = DatabaseSession(java.util.concurrent.Executors.newSingleThreadExecutor(workerThreads)) { pool }
        session.whenReady().get(2, TimeUnit.SECONDS)
        pool.events.clear()
        val started = java.util.concurrent.CountDownLatch(1)
        val release = java.util.concurrent.CountDownLatch(1)
        var thread: String? = null
        val result = session.transaction { tx ->
            thread = Thread.currentThread().name
            tx.execute("UPDATE a SET x = 1", emptyList<Any>())
            started.countDown()
            // Shutdown interrupts the worker best-effort; keep blocking like a JDBC call that ignores interrupts.
            while (release.count > 0) java.util.concurrent.locks.LockSupport.parkNanos(1_000_000)
            1
        }
        assertTrue(started.await(2, TimeUnit.SECONDS))
        val begin = System.nanoTime()
        session.close()
        assertTrue(System.nanoTime() - begin < TimeUnit.SECONDS.toNanos(1), "close returned while work was blocked")
        assertTrue(result.isCompletedExceptionally)
        release.countDown()
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2)
        while ("close" !in pool.events && System.nanoTime() < deadline) Thread.sleep(10)
        assertEquals("SneakyDB-Worker", thread)
        assertFalse("commit" in pool.events)
        assertTrue("rollback" in pool.events)
    }

    private fun notifications() = NotificationWorker({ NotificationWorkerTest.FakeConnection() }, {}, startThread = { null })

    @Test fun `listen is allowed before readiness and refused after initialization failure or shutdown`() {
        val worker = Worker()
        val session = DatabaseSession(worker, notifications()) { Pool() }
        assertDoesNotThrow { session.listen("pushes", java.util.function.Consumer {}).close() }
        worker.runNext()
        session.close()
        assertThrows(IllegalStateException::class.java) { session.listen("pushes", java.util.function.Consumer {}) }

        val failing = Worker()
        val failed = DatabaseSession(failing, notifications()) { throw IllegalArgumentException("private") }
        failing.runNext()
        val error = assertThrows(IllegalStateException::class.java) { failed.listen("pushes", java.util.function.Consumer {}) }
        assertFalse(error.message!!.contains("private"))
    }

    @Test fun `listen without a notification worker is unsupported`() {
        val session = DatabaseSession(Worker()) { Pool() }
        assertThrows(UnsupportedOperationException::class.java) { session.listen("pushes", java.util.function.Consumer {}) }
    }

    @Test fun `workers resolve the bundled driver through SneakyDB's own loader`() {
        val thread = workerThreads.newThread {}
        assertSame(DatabaseSession::class.java.classLoader, thread.contextClassLoader)
        assertTrue(thread.isDaemon)
        assertEquals("SneakyDB-Worker", thread.name)
    }

    @Test fun `unreadable or unsupported config fails readiness during adapter creation`() {
        val settings = com.sneakydb.adapter.DatabaseSettings("localhost", 5432, "minecraft", "minecraft", "password=secret", 1, 250)
        for (config in listOf(null, DatabaseConfig("mysql", settings))) {
            val session = startDatabaseSession(config) {}
            val failure = assertThrows(java.util.concurrent.ExecutionException::class.java) { session.whenReady().get(5, TimeUnit.SECONDS) }
            assertEquals("SneakyDB initialization failed during adapter creation; check adapter configuration and database access", failure.cause?.message)
            session.close()
        }
    }
}
