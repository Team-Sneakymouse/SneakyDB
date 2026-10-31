package com.sneakydb

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class DatabaseHealthTest {
    @Test fun `startup failure requests maintenance without scheduling initialization retries`() {
        val worker = DatabaseSessionTest.Worker()
        val session = DatabaseSession(worker) { throw IllegalStateException("private credentials") }
        val commands = mutableListOf<String>()
        var scheduled = false
        val health = DatabaseHealth(session.whenReady(), OutageFallback("Offline", { it() }, { commands.add(it) }),
            { error("must not probe after terminal startup failure") }, { scheduled = true; AutoCloseable {} })
        worker.runNext()
        assertEquals(listOf("cmi maintenance true Offline"), commands)
        assertFalse(scheduled)
        assertTrue(worker.tasks.isEmpty())
        health.close()
        session.close()
    }

    @Test fun `probe exceptions activate maintenance without exposing diagnostics`() {
        val commands = mutableListOf<String>()
        var check: (() -> Unit)? = null
        val health = DatabaseHealth(java.util.concurrent.CompletableFuture.completedFuture<Void>(null),
            OutageFallback("Offline", { it() }, { commands.add(it) }),
            { throw java.sql.SQLException("password=private") }, { check = it; AutoCloseable {} })
        assertDoesNotThrow { check!!() }
        assertEquals(listOf("cmi maintenance true Offline"), commands)
        health.close()
    }

    @Test fun `ordinary statement errors do not activate maintenance while independent probe is healthy`() {
        val worker = DatabaseSessionTest.Worker()
        val pool = object : com.zaxxer.hikari.HikariDataSource() {
            override fun getConnection(): java.sql.Connection = java.lang.reflect.Proxy.newProxyInstance(
                javaClass.classLoader, arrayOf(java.sql.Connection::class.java),
            ) { _, method, _ ->
                when (method.name) {
                    "isValid" -> true
                    "close" -> null
                    "prepareStatement" -> throw java.sql.SQLSyntaxErrorException("bad consumer SQL", "42601")
                    else -> error(method.name)
                }
            } as java.sql.Connection
            override fun close() {}
        }
        val session = DatabaseSession(worker) { pool }
        val commands = mutableListOf<String>()
        var check: (() -> Unit)? = null
        val health = DatabaseHealth(session.whenReady(), OutageFallback("Offline", { it() }, { commands.add(it) }),
            { true }, { check = it; AutoCloseable {} })
        worker.runNext()
        val query = session.execute("INVALID SQL", emptyList<Any>())
        worker.runNext()
        assertTrue(query.isCompletedExceptionally)
        check!!()
        assertTrue(commands.isEmpty())
        health.close()
        session.close()
    }

    @Test fun `close during in flight failing probe discards its result`() {
        val ready = java.util.concurrent.CompletableFuture.completedFuture<Void>(null)
        val entered = java.util.concurrent.CountDownLatch(1)
        val release = java.util.concurrent.CountDownLatch(1)
        val commands = mutableListOf<String>()
        var check: (() -> Unit)? = null
        val health = DatabaseHealth(ready, OutageFallback("Offline", { it() }, { commands.add(it) }), {
            entered.countDown()
            assertTrue(release.await(2, java.util.concurrent.TimeUnit.SECONDS))
            throw java.sql.SQLException("private credentials")
        }, { check = it; AutoCloseable {} })
        val thread = Thread { check!!() }.apply { start() }
        try {
            assertTrue(entered.await(2, java.util.concurrent.TimeUnit.SECONDS))
            health.close()
        } finally { release.countDown(); thread.join(2000) }
        assertFalse(thread.isAlive)
        assertTrue(commands.isEmpty())
    }

    @Test fun `close cancels checks and fences late readiness and probe results`() {
        val ready = java.util.concurrent.CompletableFuture<Void>()
        var scheduled = 0
        var cancelled = 0
        var check: (() -> Unit)? = null
        val commands = mutableListOf<String>()
        val fallback = OutageFallback("Offline", { it() }, { commands.add(it) })
        val health = DatabaseHealth(ready, fallback, { false }, {
            scheduled++; check = it; AutoCloseable { cancelled++ }
        })
        ready.complete(null)
        health.close()
        health.close()
        check!!()
        assertEquals(1, cancelled)
        assertTrue(commands.isEmpty())
        val lateReady = java.util.concurrent.CompletableFuture<Void>()
        val late = DatabaseHealth(lateReady, fallback, { false }, { scheduled++; AutoCloseable {} })
        late.close()
        lateReady.complete(null)
        assertEquals(1, scheduled)
    }

    @Test fun `successful initialization starts isolated health checks and outage latches after recovery`() {
        val worker = DatabaseSessionTest.Worker()
        val session = DatabaseSession(worker) { DatabaseSessionTest.Pool() }
        val commands = mutableListOf<String>()
        val fallback = OutageFallback("Offline", { it() }, { commands.add(it) })
        var check: (() -> Unit)? = null
        var healthy = true
        val health = DatabaseHealth(session.whenReady(), fallback, { healthy }, { check = it; AutoCloseable {} })
        assertNull(check)
        worker.runNext()
        assertNotNull(check)
        val tick = check!!
        tick()
        assertTrue(commands.isEmpty())
        // Busy consumer queue cannot delay dedicated checks.
        session.execute("queued SQL", emptyList<Any>())
        healthy = false
        tick()
        healthy = true
        tick()
        healthy = false
        tick()
        assertEquals(listOf("cmi maintenance true Offline"), commands)
        assertTrue(session.isAvailable())
        session.whenReady().join()
        health.close()
        session.close()
    }
}
