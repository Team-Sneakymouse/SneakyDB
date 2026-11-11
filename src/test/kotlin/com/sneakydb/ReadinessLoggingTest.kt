package com.sneakydb

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.logging.Handler
import java.util.logging.LogRecord
import java.util.logging.Logger

class ReadinessLoggingTest {
    private val records = mutableListOf<LogRecord>()
    private val logger = Logger.getAnonymousLogger().apply {
        useParentHandlers = false
        addHandler(object : Handler() {
            override fun publish(record: LogRecord) { records.add(record) }
            override fun flush() {}
            override fun close() {}
        })
    }

    @Test fun `pending shutdown produces no severe initialization log`() {
        val worker = DatabaseSessionTest.Worker()
        val session = DatabaseSession(worker) { DatabaseSessionTest.Pool() }
        session.whenReady().whenComplete { _, failure -> logReadinessFailure(logger, failure) }
        session.close()
        assertTrue(records.isEmpty())
    }

    @Test fun `notification subscriptions leave readiness logging unchanged`() {
        val notifications = NotificationWorker({ NotificationWorkerTest.FakeConnection() }, { logger.warning(it) }, startThread = { null })
        val worker = DatabaseSessionTest.Worker()
        val session = DatabaseSession(worker, notifications) { DatabaseSessionTest.Pool() }
        session.whenReady().whenComplete { _, failure -> logReadinessFailure(logger, failure) }
        val subscription = session.listen("pushes", java.util.function.Consumer {})
        session.close()
        subscription.close()
        assertTrue(records.isEmpty(), "shutdown with a subscription logs nothing")

        val failing = DatabaseSessionTest.Worker()
        val failed = DatabaseSession(failing, NotificationWorker({ error("x") }, { logger.warning(it) }, startThread = { null })) {
            error("password=secret")
        }
        failed.whenReady().whenComplete { _, failure -> logReadinessFailure(logger, failure) }
        failing.runNext()
        assertThrows(IllegalStateException::class.java) { failed.listen("pushes", java.util.function.Consumer {}) }
        assertEquals(listOf("SneakyDB initialization failed during adapter creation; check adapter configuration and database access"),
            records.map { it.message }, "one sanitized severe line, no notification warnings")
        failed.close()
    }

    @Test fun `unknown error text and driver causes are never logged`() {
        logReadinessFailure(logger, java.util.concurrent.CompletionException(
            IllegalStateException("password=secret", RuntimeException("private driver cause"))))
        assertEquals("SneakyDB initialization failed; check database configuration and access.", records.single().message)
        assertNull(records.single().thrown)
    }

    @Test fun `wrapped readiness failure logs sanitized initialization phase`() {
        for (phase in listOf("adapter creation", "connection acquisition", "connection validation")) {
            records.clear()
            val worker = DatabaseSessionTest.Worker()
            val pool = DatabaseSessionTest.Pool().apply {
                if (phase == "connection acquisition") failure = IllegalStateException("password=secret")
                if (phase == "connection validation") onValidate = { error("password=secret") }
            }
            val session = DatabaseSession(worker) {
                if (phase == "adapter creation") error("password=secret")
                pool
            }
            session.whenReady().whenComplete { _, failure -> logReadinessFailure(logger, failure) }
            worker.runNext()
            assertEquals(1, records.size)
            assertEquals("SneakyDB initialization failed during $phase; check adapter configuration and database access", records.single().message)
            assertNull(records.single().thrown)
            session.close()
        }
    }

    @Test fun `plain severe sink used on Velocity gets the same single line and nothing on shutdown`() {
        val lines = mutableListOf<String>()
        val worker = DatabaseSessionTest.Worker()
        val failed = DatabaseSession(worker) { error("password=secret") }
        failed.whenReady().whenComplete { _, failure -> logReadinessFailure(lines::add, failure) }
        worker.runNext()
        failed.close()
        assertEquals(listOf("SneakyDB initialization failed during adapter creation; check adapter configuration and database access"), lines)

        lines.clear()
        val pending = DatabaseSession(DatabaseSessionTest.Worker()) { DatabaseSessionTest.Pool() }
        pending.whenReady().whenComplete { _, failure -> logReadinessFailure(lines::add, failure) }
        pending.close()
        assertTrue(lines.isEmpty())
    }
}
