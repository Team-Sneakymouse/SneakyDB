package com.sneakydb

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class HealthWorkerTest {
    @Test fun `health pool has independent bounded capacity and network waits`() {
        val settings = com.sneakydb.adapter.DatabaseSettings("localhost", 5432, "test", "user", "secret", 20, 30000)
        createHealthSource(settings).use { pool ->
            assertEquals(1, pool.maximumPoolSize)
            assertEquals(0, pool.minimumIdle)
            assertEquals(5000, pool.connectionTimeout)
            assertEquals("5", pool.dataSourceProperties.getProperty("socketTimeout"))
            assertEquals("5", pool.dataSourceProperties.getProperty("connectTimeout"))
            assertFalse(pool.isRunning)
        }
    }

    @Test fun `closing idle pool never runs JDBC close on caller thread`() {
        val closed = java.util.concurrent.atomic.AtomicReference<Thread>()
        val pool = object : com.zaxxer.hikari.HikariDataSource() {
            override fun getConnection() = DatabaseSessionTest.Pool().connection
            override fun close() { closed.set(Thread.currentThread()) }
        }
        val validated = CountDownLatch(1)
        val worker = HealthWorker({ pool }, 10000)
        worker.schedule { worker.probe(); validated.countDown() }
        assertTrue(validated.await(2, TimeUnit.SECONDS))
        Thread.sleep(100) // Let the scheduled callback return: exercise idle-executor shutdown.
        val caller = Thread.currentThread()
        worker.close()
        assertTrue(worker.awaitTermination(2, TimeUnit.SECONDS))
        assertNotNull(closed.get())
        assertNotSame(caller, closed.get())
    }

    @Test fun `dedicated worker validates off caller thread and closes its pool`() {
        val caller = Thread.currentThread()
        val validated = CountDownLatch(1)
        val pool = DatabaseSessionTest.Pool().apply {
            onValidate = { assertNotSame(caller, Thread.currentThread()); validated.countDown() }
        }
        val worker = HealthWorker({ assertNotSame(caller, Thread.currentThread()); pool }, 1000)
        val handle = worker.schedule { assertTrue(worker.probe()) }
        assertTrue(validated.await(2, TimeUnit.SECONDS))
        handle.close()
        worker.close()
        assertTrue(worker.awaitTermination(2, TimeUnit.SECONDS))
        assertTrue(pool.closed)
        assertTrue(pool.connectionClosed)
        assertEquals(5, pool.timeout)
    }
}
