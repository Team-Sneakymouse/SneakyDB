package com.sneakydb

import com.sneakydb.adapter.DatabaseSettings
import com.sneakydb.adapter.openPostgreSqlNotificationConnection
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

/**
 * Runs only against a disposable PostgreSQL named by SNEAKYDB_TEST_PG_HOST (plus optional _PORT, _DB, _USER,
 * _PASSWORD), for example `docker run --rm -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:17-alpine`.
 */
class PostgreSqlNotificationTest {
    private val env = System.getenv()
    private val settings = env["SNEAKYDB_TEST_PG_HOST"]?.let { host ->
        DatabaseSettings(
            host, env["SNEAKYDB_TEST_PG_PORT"]?.toInt() ?: 5432, env["SNEAKYDB_TEST_PG_DB"] ?: "postgres",
            env["SNEAKYDB_TEST_PG_USER"] ?: "postgres", env["SNEAKYDB_TEST_PG_PASSWORD"] ?: "test", 1, 5000,
        )
    }

    private fun sql(statement: String) {
        val s = settings!!
        java.sql.DriverManager.getConnection("jdbc:postgresql://${s.host}:${s.port}/${s.database}", s.username, s.password)
            .use { it.createStatement().use { st -> st.execute(statement) } }
    }

    @Test fun `delivers notifications and resubscribes after the server drops the connection`() {
        assumeTrue(settings != null, "SNEAKYDB_TEST_PG_HOST not set")
        val payloads = LinkedBlockingQueue<String>()
        val connects = LinkedBlockingQueue<Unit>()
        val warnings = LinkedBlockingQueue<String>()
        openPostgreSqlNotificationConnection(settings!!).use { connection ->
            assertEquals(15_000, (connection as com.sneakydb.adapter.PostgreSqlNotificationConnection).networkTimeoutMs(), "reads are bounded")
        }
        val worker = NotificationWorker({ openPostgreSqlNotificationConnection(settings!!) }, { warnings.add(it) }, pollMs = 100)
        try {
            worker.subscribe("sneakydb_test", Consumer { payloads.add(it) }, Runnable { connects.add(Unit) })
            assertNotNull(connects.poll(10, TimeUnit.SECONDS), "first connect")
            sql("select pg_notify('sneakydb_test', 'one')")
            assertEquals("one", payloads.poll(10, TimeUnit.SECONDS))

            sql("select pg_terminate_backend(pid) from pg_stat_activity where application_name = 'SneakyDB-Notifications'")
            assertNotNull(connects.poll(15, TimeUnit.SECONDS), "reconnect")
            val lost = warnings.poll(1, TimeUnit.SECONDS)!!
            assertTrue(lost.startsWith("SneakyDB notification connection lost; reconnecting (") && "SQL state" in lost, lost)
            sql("select pg_notify('sneakydb_test', 'two')")
            assertEquals("two", payloads.poll(10, TimeUnit.SECONDS))
        } finally {
            worker.close()
        }
    }
}
