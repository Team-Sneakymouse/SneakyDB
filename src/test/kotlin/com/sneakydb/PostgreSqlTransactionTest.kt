package com.sneakydb

import com.sneakydb.adapter.DatabaseSettings
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import java.sql.SQLException
import java.util.concurrent.CompletionException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Runs only against a disposable PostgreSQL named by SNEAKYDB_TEST_PG_HOST (plus optional _PORT, _DB, _USER,
 * _PASSWORD), like [PostgreSqlNotificationTest]. The pool has one connection, so every operation after a transaction
 * reuses the transaction's connection and would inherit any state it left behind.
 */
class PostgreSqlTransactionTest {
    private val env = System.getenv()
    private val settings = env["SNEAKYDB_TEST_PG_HOST"]?.let { host ->
        DatabaseSettings(
            host, env["SNEAKYDB_TEST_PG_PORT"]?.toInt() ?: 5432, env["SNEAKYDB_TEST_PG_DB"] ?: "postgres",
            env["SNEAKYDB_TEST_PG_USER"] ?: "postgres", env["SNEAKYDB_TEST_PG_PASSWORD"] ?: "test", 1, 5000,
        )
    }
    private val table = "sneakydb_transaction_test_${System.nanoTime()}"

    private fun raw(): java.sql.Connection {
        val s = settings!!
        return java.sql.DriverManager.getConnection("jdbc:postgresql://${s.host}:${s.port}/${s.database}", s.username, s.password)
    }

    private fun <T> rawQuery(sql: String, read: (java.sql.ResultSet) -> T): T =
        raw().use { c -> c.createStatement().use { s -> s.executeQuery(sql).use(read) } }

    private fun ids(): List<Int> = rawQuery("select id from $table order by id") { r ->
        buildList { while (r.next()) add(r.getInt(1)) }
    }

    /** States of every other client backend in this database, as seen from a separate connection. */
    private fun otherBackends(): List<String> = rawQuery(
        "select coalesce(state, '') from pg_stat_activity where backend_type = 'client backend' " +
            "and datname = current_database() and pid <> pg_backend_pid()",
    ) { r -> buildList { while (r.next()) add(r.getString(1)) } }

    private fun withSession(test: (DatabaseSession) -> Unit) {
        assumeTrue(settings != null, "SNEAKYDB_TEST_PG_HOST not set")
        raw().use { c -> c.createStatement().use { it.execute("create table $table (id int primary key)") } }
        val session = startDatabaseSession(DatabaseConfig("postgresql", settings!!)) {}
        try {
            session.whenReady().get(10, TimeUnit.SECONDS)
            test(session)
        } finally {
            session.close()
            raw().use { c -> c.createStatement().use { it.execute("drop table if exists $table") } }
        }
    }

    /** A plain execute on the reused connection is visible elsewhere at once only in autocommit with no transaction open. */
    private fun assertConnectionClean(session: DatabaseSession, id: Int) {
        assertFalse(otherBackends().any { it.startsWith("idle in transaction") }, "no transaction left open")
        session.execute("insert into $table values (?)", listOf(id)).get(10, TimeUnit.SECONDS)
        assertTrue(id in ids(), "autocommit restored")
    }

    @Test fun `commit makes all writes visible together`() = withSession { session ->
        val result = session.transaction { tx ->
            tx.execute("insert into $table values (?)", listOf(1))
            tx.execute("insert into $table values (?)", listOf(2))
            val uncommitted = ids()
            val own = tx.query("select count(*) as n from $table", emptyList<Any>()).single()["n"]
            uncommitted to own
        }.get(10, TimeUnit.SECONDS)
        assertEquals(emptyList<Int>(), result.first, "other connections see nothing before the commit")
        assertEquals(2L, result.second, "the transaction sees its own writes")
        assertEquals(listOf(1, 2), ids())
        assertConnectionClean(session, 10)
    }

    @Test fun `an exception from work rolls every write back`() = withSession { session ->
        val thrown = IllegalStateException("consumer failure")
        val future = session.transaction<Int> { tx ->
            tx.execute("insert into $table values (?)", listOf(1))
            throw thrown
        }
        val error = assertThrows(CompletionException::class.java) { future.join() }
        assertSame(thrown, error.cause)
        assertEquals(emptyList<Int>(), ids())
        assertConnectionClean(session, 10)
    }

    @Test fun `a failed statement fails the future with its SQL state and rolls back`() = withSession { session ->
        val future = session.transaction { tx ->
            tx.execute("insert into $table values (?)", listOf(1))
            tx.execute("insert into $table values (?)", listOf(1))
        }
        val error = assertThrows(CompletionException::class.java) { future.join() }
        assertEquals("23505", (error.cause as SQLException).sqlState)
        assertEquals(emptyList<Int>(), ids())
        assertConnectionClean(session, 10)
    }

    @Test fun `shutdown during work rolls the transaction back`() {
        assumeTrue(settings != null, "SNEAKYDB_TEST_PG_HOST not set")
        raw().use { c -> c.createStatement().use { it.execute("create table $table (id int primary key)") } }
        try {
            val session = startDatabaseSession(DatabaseConfig("postgresql", settings!!)) {}
            session.whenReady().get(10, TimeUnit.SECONDS)
            val written = CountDownLatch(1)
            val release = CountDownLatch(1)
            val future = session.transaction { tx ->
                tx.execute("insert into $table values (?)", listOf(1))
                written.countDown()
                while (release.count > 0) java.util.concurrent.locks.LockSupport.parkNanos(1_000_000)
                1
            }
            assertTrue(written.await(10, TimeUnit.SECONDS))
            session.close()
            assertTrue(future.isCompletedExceptionally)
            release.countDown()
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15)
            while (otherBackends().isNotEmpty() && System.nanoTime() < deadline) Thread.sleep(50)
            assertEquals(emptyList<String>(), otherBackends(), "pool closed")
            assertEquals(emptyList<Int>(), ids())
        } finally {
            raw().use { c -> c.createStatement().use { it.execute("drop table if exists $table") } }
        }
    }
}
