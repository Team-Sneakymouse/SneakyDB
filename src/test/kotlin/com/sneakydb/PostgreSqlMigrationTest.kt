package com.sneakydb

import com.sneakydb.MigrationScriptsTest.Companion.scripts
import com.sneakydb.adapter.DatabaseSettings
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Runs only against a disposable PostgreSQL named by SNEAKYDB_TEST_PG_HOST (plus optional _PORT, _DB, _USER,
 * _PASSWORD), like [PostgreSqlTransactionTest]. The user must be allowed to create databases: each test gets a new
 * database, so the `sneakydb` schema starts out as each test needs it, and drops it afterwards.
 */
class PostgreSqlMigrationTest {
    private val env = System.getenv()
    private val admin = env["SNEAKYDB_TEST_PG_HOST"]?.let { host ->
        DatabaseSettings(
            host, env["SNEAKYDB_TEST_PG_PORT"]?.toInt() ?: 5432, env["SNEAKYDB_TEST_PG_DB"] ?: "postgres",
            env["SNEAKYDB_TEST_PG_USER"] ?: "postgres", env["SNEAKYDB_TEST_PG_PASSWORD"] ?: "test", 2, 5000,
        )
    }
    private val database = "sneakydb_migration_test_${System.nanoTime()}"
    private val settings get() = admin!!.copy(database = database)
    private val sessions = mutableListOf<DatabaseSession>()
    private val coordinators = mutableListOf<MigrationCoordinator>()
    private val info = CopyOnWriteArrayList<String>()
    private val severe = CopyOnWriteArrayList<String>()

    private fun connect(name: String = database) =
        java.sql.DriverManager.getConnection("jdbc:postgresql://${admin!!.host}:${admin.port}/$name", admin.username, admin.password)

    private fun sql(statement: String) = connect().use { c -> c.createStatement().use { it.execute(statement) } }

    private fun <T> rows(query: String, read: (java.sql.ResultSet) -> T): List<T> =
        connect().use { c -> c.createStatement().use { s -> s.executeQuery(query).use { r -> buildList { while (r.next()) add(read(r)) } } } }

    private fun versions(): List<String> =
        rows("select plugin || ':' || version from sneakydb.schema_migrations order by applied_at, plugin, version") { it.getString(1) }

    private fun exists(relation: String) = rows("select to_regclass('$relation') is not null") { it.getBoolean(1) }.single()

    @BeforeEach fun createDatabase() {
        assumeTrue(admin != null, "SNEAKYDB_TEST_PG_HOST not set")
        connect(admin!!.database).use { c -> c.createStatement().use { it.execute("create database $database") } }
        sql("create schema sneakydb")
    }

    @AfterEach fun dropDatabase() {
        if (admin == null) return
        coordinators.forEach { it.close() }
        sessions.forEach { it.close() }
        Thread.sleep(200) // pool closure runs on its own thread
        connect(admin.database).use { c -> c.createStatement().use { it.execute("drop database if exists $database with (force)") } }
    }

    private fun session(mode: MigrationMode = MigrationMode.APPLY, lockTimeout: Int = 10): MigrationCoordinator {
        val session = startDatabaseSession(DatabaseConfig("postgresql", settings), MigrationSettings(mode, lockTimeout, "starting"), info::add, severe::add) {}
        sessions.add(session)
        return session.migrations!!
    }

    private fun MigrationCoordinator.add(plugin: String, vararg files: Pair<String, String>): CompletableFuture<Int> =
        register(plugin) { scripts(plugin, *files) }

    private fun failure(future: CompletableFuture<*>): String {
        runCatching { future.get(30, TimeUnit.SECONDS) }
        return assertThrows(CompletionException::class.java) { future.join() }.cause!!.message!!
    }

    private fun CompletableFuture<Int>.await() = get(30, TimeUnit.SECONDS)

    private val plotsScripts = arrayOf(
        "V001__init.sql" to "create table plots (id integer primary key, owner text);\ncreate index plots_owner on plots (owner);\n",
        "V002__seed.sql" to "insert into plots values (1, 'a');\r\ninsert into plots values (2, 'b');\r\n",
    )

    @Test fun `apply from empty creates the version table and records every script`() {
        assertFalse(exists("sneakydb.schema_migrations"))
        val coordinator = session()
        val plots = coordinator.add("plots", *plotsScripts)
        coordinator.registrationClosed()
        assertEquals(2, plots.await())
        assertTrue(coordinator.startup.get(30, TimeUnit.SECONDS))
        assertEquals(listOf("plots:1", "plots:2"), versions())
        assertEquals(listOf(2), rows("select count(*) from plots") { it.getInt(1) })
        val stored = rows("select name, checksum from sneakydb.schema_migrations order by version") { it.getString(1) to it.getString(2) }
        assertEquals(listOf("V001__init.sql", "V002__seed.sql"), stored.map { it.first })
        assertEquals(scripts("plots", *plotsScripts).map { it.checksum }, stored.map { it.second })
        assertTrue(info.any { it.startsWith("Applied migration plots:V001__init.sql in ") })
        assertTrue(severe.isEmpty(), severe.toString())
    }

    @Test fun `a restart with nothing pending changes nothing`() {
        val first = session()
        val initial = first.add("plots", *plotsScripts)
        first.registrationClosed()
        initial.await()
        val before = rows("select version, applied_at from sneakydb.schema_migrations order by version") { it.getInt(1) to it.getTimestamp(2) }
        info.clear()
        val second = session()
        val again = second.add("plots", *plotsScripts)
        second.registrationClosed()
        assertEquals(2, again.await())
        assertEquals(before, rows("select version, applied_at from sneakydb.schema_migrations order by version") { it.getInt(1) to it.getTimestamp(2) })
        assertTrue(info.last().contains("0 scripts applied, 1 of 1 plugins ready"), info.last())
    }

    @Test fun `a failing script is rolled back entirely and reports PostgreSQL's SQL state`() {
        val coordinator = session()
        val plots = coordinator.add("plots",
            "V001__init.sql" to "create table plots (id integer primary key);",
            "V002__broken.sql" to "create table plot_notes (id integer);\ninsert into plots values (1);\ninsert into missing_table values (1);",
            "V003__later.sql" to "create table plot_later (id integer);",
        )
        coordinator.registrationClosed()
        val message = failure(plots)
        assertTrue(message.startsWith("Migrations for plots failed at V002__broken.sql: SQL state 42P01: ERROR: relation \"missing_table\" does not exist"), message)
        assertEquals(listOf(message), severe)
        assertEquals(listOf("plots:1"), versions())
        assertFalse(exists("plot_notes"), "the script's earlier statements were rolled back")
        assertEquals(listOf(0), rows("select count(*) from plots") { it.getInt(1) })
        assertFalse(exists("plot_later"), "later scripts were skipped")
        assertFalse(coordinator.startup.get(30, TimeUnit.SECONDS))
    }

    @Test fun `session settings a script changes don't leak into the pool`() {
        val coordinator = session()
        val plots = coordinator.add("plots", "V001__init.sql" to "set search_path = pg_catalog;\nset application_name = 'migrated';\ncreate table public.plots (id int);")
        coordinator.registrationClosed()
        plots.await()
        val session = sessions.single()
        // The pool has two connections; ask both.
        val seen = (1..4).map { session.query("select current_setting('search_path') as path", emptyList<Any>()).get(10, TimeUnit.SECONDS).single()["path"] }
        assertTrue(seen.none { it == "pg_catalog" }, seen.toString())
    }

    private val alphaScripts = arrayOf(
        "V001__owners.sql" to "create table alpha_owner (id integer primary key);",
        "V002__drop_key.sql" to "-- requires: beta >= 1\nalter table beta_item drop constraint beta_item_owner_fk;\nalter table alpha_owner alter column id type bigint;",
        "V003__restore_key.sql" to "-- requires: beta >= 2\nalter table beta_item add constraint beta_item_owner_fk foreign key (owner) references alpha_owner (id);",
    )
    private val betaScripts = arrayOf(
        "V001__items.sql" to "-- requires: alpha >= 1\ncreate table beta_item (id integer primary key, owner integer constraint beta_item_owner_fk references alpha_owner (id));",
        "V002__retype.sql" to "-- requires: alpha >= 2\nalter table beta_item alter column owner type bigint;",
    )

    private fun columnType(table: String, column: String) =
        rows("select data_type from information_schema.columns where table_name = '$table' and column_name = '$column'") { it.getString(1) }.single()

    @Test fun `two plugins change a shared relation in interleaved steps`() {
        val coordinator = session()
        val beta = coordinator.add("beta", *betaScripts)
        val alpha = coordinator.add("alpha", *alphaScripts)
        coordinator.registrationClosed()
        assertEquals(3, alpha.await())
        assertEquals(2, beta.await())
        assertEquals(listOf("alpha:1", "beta:1", "alpha:2", "beta:2", "alpha:3"), versions())
        assertEquals("bigint", columnType("beta_item", "owner"))
        assertEquals(listOf(true), rows("select exists (select 1 from pg_constraint where conname = 'beta_item_owner_fk')") { it.getBoolean(1) })
    }

    @Test fun `merging only one side of the interleaved change runs nothing of it`() {
        val first = session()
        val alpha1 = first.add("alpha", alphaScripts[0])
        val beta1 = first.add("beta", betaScripts[0])
        first.registrationClosed()
        alpha1.await(); beta1.await()

        val onlyAlpha = session()
        val alpha = onlyAlpha.add("alpha", *alphaScripts)
        val beta = onlyAlpha.add("beta", betaScripts[0])
        val other = onlyAlpha.add("other", "V001__x.sql" to "create table other_table (id int);")
        onlyAlpha.registrationClosed()
        assertTrue(failure(alpha).contains("needs requires: beta >= 2"))
        assertEquals(1, beta.await())
        assertEquals(1, other.await())
        assertEquals(listOf("alpha:1", "beta:1", "other:1"), versions().sorted())
        assertEquals("integer", columnType("alpha_owner", "id"), "alpha 2 did not run")
        assertFalse(onlyAlpha.startup.get(30, TimeUnit.SECONDS))

        val onlyBeta = session()
        val alphaOld = onlyBeta.add("alpha", alphaScripts[0])
        val betaNew = onlyBeta.add("beta", *betaScripts)
        onlyBeta.registrationClosed()
        assertEquals(1, alphaOld.await())
        assertTrue(failure(betaNew).contains("needs requires: alpha >= 2"))
        assertEquals("integer", columnType("beta_item", "owner"), "beta 2 did not run")
    }

    @Test fun `two servers racing on one database apply every script exactly once`() {
        val left = session()
        val right = session()
        val futures = listOf(left, right).flatMap { coordinator ->
            listOf(coordinator.add("alpha", *alphaScripts), coordinator.add("beta", *betaScripts), coordinator.add("plots", *plotsScripts))
        }
        val start = CountDownLatch(1)
        val threads = listOf(left, right).map { coordinator -> Thread { start.await(); coordinator.registrationClosed() }.apply { start() } }
        start.countDown()
        threads.forEach { it.join() }
        assertEquals(listOf(3, 2, 2, 3, 2, 2), futures.map { it.await() })
        val recorded = versions()
        assertEquals(7, recorded.size)
        assertEquals(recorded.toSet().size, recorded.size, "no script recorded twice")
        assertEquals(listOf(2), rows("select count(*) from plots") { it.getInt(1) }, "seed inserted once")
        assertTrue(severe.isEmpty(), severe.toString())
        assertTrue(listOf(left, right).all { it.startup.get(30, TimeUnit.SECONDS) })
    }

    @Test fun `an operator row with a null checksum counts as applied and gets the checksum in apply mode`() {
        sql("create table plots (id integer primary key, owner text)")
        sql("""create table sneakydb.schema_migrations (plugin text not null, version integer not null, name text not null,
               checksum text, applied_at timestamptz not null default now(), primary key (plugin, version))""")
        sql("insert into sneakydb.schema_migrations (plugin, version, name) values ('plots', 1, 'existing tables')")

        val verify = session(MigrationMode.VERIFY)
        val verified = verify.add("plots", plotsScripts[0])
        verify.registrationClosed()
        assertEquals(1, verified.await())
        assertEquals(listOf<String?>(null), rows("select checksum from sneakydb.schema_migrations") { it.getString(1) }, "verify writes nothing")

        val apply = session()
        val applied = apply.add("plots", *plotsScripts)
        apply.registrationClosed()
        assertEquals(2, applied.await(), "V001 was not run again (its create table would fail)")
        assertEquals(scripts("plots", *plotsScripts).map { it.checksum },
            rows("select checksum from sneakydb.schema_migrations order by version") { it.getString(1) })
    }

    @Test fun `verify mode fails pending plugins and leaves the database untouched`() {
        val coordinator = session(MigrationMode.VERIFY)
        val plots = coordinator.add("plots", *plotsScripts)
        coordinator.registrationClosed()
        assertTrue(failure(plots).contains("not applied, and this server runs migrations in verify mode"))
        assertFalse(exists("sneakydb.schema_migrations"), "verify mode does not create the table")
        assertFalse(exists("plots"))
    }

    @Test fun `the database being newer than the build fails the plugin`() {
        val first = session()
        first.add("plots", *plotsScripts).also { first.registrationClosed() }.await()
        val older = session()
        val plots = older.add("plots", plotsScripts[0])
        older.registrationClosed()
        assertTrue(failure(plots).contains("the database is newer than this build"))
    }

    @Test fun `an edited script fails its plugin`() {
        val first = session()
        first.add("plots", *plotsScripts).also { first.registrationClosed() }.await()
        val edited = session()
        val plots = edited.add("plots", plotsScripts[0], "V002__seed.sql" to "insert into plots values (3, 'c');")
        edited.registrationClosed()
        assertTrue(failure(plots).contains("V002__seed.sql differs from the script that was applied"))
    }

    @Test fun `a script waiting too long for a lock fails instead of freezing`() {
        sql("create table busy (id int)")
        connect().use { holder ->
            holder.autoCommit = false
            holder.createStatement().use { it.execute("lock table busy in access exclusive mode") }
            val coordinator = session(lockTimeout = 1)
            val plots = coordinator.add("plots", "V001__alter.sql" to "alter table busy add column name text;")
            coordinator.registrationClosed()
            assertTrue(failure(plots).contains("SQL state 55P03"))
            holder.rollback()
        }
        assertEquals(emptyList<String>(), versions())
    }

    @Test fun `a missing sneakydb schema fails every plugin with the statements to run`() {
        sql("drop schema sneakydb")
        for (mode in MigrationMode.entries) {
            val coordinator = session(mode)
            val plots = coordinator.add("plots", *plotsScripts)
            val other = coordinator.add("other", "V1__x.sql" to "select 1;")
            coordinator.registrationClosed()
            for (future in listOf(plots, other)) {
                val message = failure(future)
                assertTrue(message.contains("the schema sneakydb does not exist; as the database owner run: create schema sneakydb; " +
                    "grant usage, create on schema sneakydb to ${admin!!.username};"), message)
            }
        }
        assertEquals(listOf(false), rows("select exists (select 1 from pg_namespace where nspname = 'sneakydb')") { it.getBoolean(1) })
        assertFalse(exists("plots"))
    }

    @Test fun `a role that may not use the schema is told what to grant`() {
        sql("create role sneakydb_limited login password 'limited'")
        try {
            sql("grant connect on database $database to sneakydb_limited")
            val limited = settings.copy(username = "sneakydb_limited", password = "limited")
            val session = startDatabaseSession(DatabaseConfig("postgresql", limited), MigrationSettings.DEFAULTS, info::add, severe::add) {}
            sessions.add(session)
            val coordinator = session.migrations!!
            val plots = coordinator.add("plots", *plotsScripts)
            coordinator.registrationClosed()
            val message = failure(plots)
            assertTrue(message.contains("role sneakydb_limited may not use the schema sneakydb; as the database owner run: " +
                "grant usage, create on schema sneakydb to sneakydb_limited;"), message)
            assertTrue((info + severe).none { "limited'" in it || "password" in it })
        } finally {
            sessions.forEach { it.close() }
            sessions.clear()
            Thread.sleep(200)
            connect(admin!!.database).use { c -> c.createStatement().use { it.execute("drop database if exists $database with (force)") } }
            sql2("drop role if exists sneakydb_limited")
        }
    }

    /**
     * A server in apply mode whose store runs [between] once, after the read it plans from and before its first write:
     * what another server does while this one plans.
     */
    private fun interleaved(between: () -> Unit): MigrationCoordinator {
        val session = startDatabaseSession(DatabaseConfig("postgresql", settings), MigrationSettings.DEFAULTS, info::add, severe::add) {}
        sessions.add(session)
        val inner = PostgreSqlMigrationStore(session)
        var first = true
        val store = object : MigrationStore by inner {
            override fun readApplied(mode: MigrationMode): Map<String, List<AppliedVersion>> =
                inner.readApplied(mode).also { if (first) { first = false; between() } }
        }
        return MigrationCoordinator(MigrationSettings.DEFAULTS, store, session::whenReady, info::add, severe::add).also { coordinators.add(it) }
    }

    /** Another server applies [files] as [plugin]'s scripts and finishes. */
    private fun otherServerApplies(plugin: String, vararg files: Pair<String, String>) {
        val other = session()
        val done = other.add(plugin, *files)
        other.registrationClosed()
        done.await()
    }

    private val plotsV3 = "V003__more.sql" to "create table plot_more (id integer);"

    @Test fun `an older jar fails when another server moved the database past it while it planned`() {
        val older = interleaved { otherServerApplies("plots", *plotsScripts, plotsV3) }
        val plots = older.add("plots", *plotsScripts)
        older.registrationClosed()
        val message = failure(plots)
        assertTrue(message.contains("the database is newer than this build: it is at version 3, this build's scripts go up to 2"), message)
        assertFalse(older.startup.get(30, TimeUnit.SECONDS))

        sql("drop table plot_more, plots; delete from sneakydb.schema_migrations")
        val oldest = interleaved { otherServerApplies("plots", *plotsScripts) }
        val v1Only = oldest.add("plots", plotsScripts[0])
        oldest.registrationClosed()
        assertTrue(failure(v1Only).contains("it is at version 2, this build's scripts go up to 1"))
    }

    private fun operatorRow() {
        sql("""create table sneakydb.schema_migrations (plugin text not null, version integer not null, name text not null,
               checksum text, applied_at timestamptz not null default now(), primary key (plugin, version))""")
        sql("insert into sneakydb.schema_migrations (plugin, version, name) values ('plots', 1, 'existing tables')")
    }

    @Test fun `a checksum another server filled in from a different script fails the plugin`() {
        operatorRow()
        val server = interleaved { sql("update sneakydb.schema_migrations set checksum = '${"0".repeat(64)}' where plugin = 'plots'") }
        val plots = server.add("plots", plotsScripts[0])
        server.registrationClosed()
        val message = failure(plots)
        assertTrue(message.contains("V001__init.sql differs from the script that was applied (checksum mismatch)"), message)
        assertEquals(listOf("0".repeat(64)), rows("select checksum from sneakydb.schema_migrations") { it.getString(1) })
        assertFalse(server.startup.get(30, TimeUnit.SECONDS))
    }

    @Test fun `a plugin with nothing pending is ready only after its checksum fill-in succeeded`() {
        operatorRow()
        sql("create role sneakydb_noupdate login password 'noupdate'")
        try {
            sql("grant connect on database $database to sneakydb_noupdate")
            sql("grant usage on schema sneakydb to sneakydb_noupdate")
            sql("grant select, insert on sneakydb.schema_migrations to sneakydb_noupdate")
            val limited = settings.copy(username = "sneakydb_noupdate", password = "noupdate")
            val session = startDatabaseSession(DatabaseConfig("postgresql", limited), MigrationSettings.DEFAULTS, info::add, severe::add) {}
            sessions.add(session)
            val coordinator = session.migrations!!
            val plots = coordinator.add("plots", plotsScripts[0])
            coordinator.registrationClosed()
            val message = failure(plots)
            assertTrue(message.contains("SQL state 42501"), message)
            assertFalse(coordinator.startup.get(30, TimeUnit.SECONDS))
        } finally {
            sessions.forEach { it.close() }
            sessions.clear()
            Thread.sleep(200)
            connect(admin!!.database).use { c -> c.createStatement().use { it.execute("drop database if exists $database with (force)") } }
            sql2("drop role if exists sneakydb_noupdate")
        }
    }

    /** A script registered as is, without the checks parsing does: what a script that slips past them would do. */
    private fun MigrationCoordinator.unchecked(plugin: String, name: String, text: String): CompletableFuture<Int> =
        register(plugin) { listOf(MigrationScript(1, name, text, sha256(text), emptyList())) }

    @Test fun `a script that commits and then fails is reported as possibly half applied`() {
        val coordinator = session()
        val plots = coordinator.unchecked("plots", "V001__init.sql", "create table plots_early (id int);\ncommit;\ninsert into missing_table values (1);")
        coordinator.registrationClosed()
        val message = failure(plots)
        assertTrue(message.contains("V001__init.sql ended the transaction SneakyDB runs it in"), message)
        assertTrue(message.contains("may hold part of its changes"), message)
        assertTrue(message.contains("SQL state 42P01"), message)
        assertEquals(emptyList<String>(), versions())
    }

    @Test fun `a script that rolls back and continues is not recorded`() {
        val coordinator = session()
        val plots = coordinator.unchecked("plots", "V001__init.sql", "create table plots_a (id int);\nrollback;\ncreate table plots_b (id int);")
        coordinator.registrationClosed()
        assertTrue(failure(plots).contains("V001__init.sql ended the transaction SneakyDB runs it in"))
        assertEquals(emptyList<String>(), versions())
        assertTrue(exists("plots_b"), "the part after the rollback ran in a transaction of its own: the database holds part of the script")
    }

    @Test fun `a script that commits, begins again and then fails is reported as possibly half applied`() {
        val coordinator = session()
        val plots = coordinator.unchecked("plots", "V001__init.sql",
            "create table plots_early (id int);\ncommit;\nbegin;\ninsert into missing_table values (1);")
        coordinator.registrationClosed()
        val message = failure(plots)
        assertTrue(message.contains("may hold part of its changes"), message)
        assertTrue(message.contains("SQL state 42P01"), message)
        assertEquals(emptyList<String>(), versions())
        assertTrue(exists("plots_early"), "the part before the commit stayed: the database holds part of the script")
        // The pool still works for the next script.
        val next = coordinator.add("other", "V001__other.sql" to "create table other_table (id int);")
        assertEquals(1, next.await())
    }

    @Test fun `scripts don't run when standard_conforming_strings is off on their connection`() {
        sql("alter database $database set standard_conforming_strings = off")
        val coordinator = session()
        val plots = coordinator.add("plots", "V001__init.sql" to "create table plots (id int);")
        coordinator.registrationClosed()
        val message = failure(plots)
        assertTrue(message.contains("standard_conforming_strings is off"), message)
        assertFalse(exists("plots"))
        assertEquals(emptyList<String>(), versions())
    }

    @Test fun `a script that turns standard_conforming_strings off fails at its next statement`() {
        val coordinator = session()
        // With the setting off, PostgreSQL reads \' as an escaped quote: the string never ends.
        val plots = coordinator.add("plots", "V001__init.sql" to
            "create table plots (id int);\nset standard_conforming_strings = off;\nselect '\\'; select 1; --';\ncreate table plots_after (id int);")
        coordinator.registrationClosed()
        val message = failure(plots)
        assertTrue(message.contains("SQL state 42601"), message)
        assertFalse(message.contains("may hold part"), message)
        assertFalse(exists("plots"))
        assertFalse(exists("plots_after"))
        assertEquals(emptyList<String>(), versions())
    }

    @Test fun `a column named end inside a BEGIN ATOMIC body runs`() {
        val coordinator = session()
        val plots = coordinator.add("plots", "V001__functions.sql" to
            "create table plots_ends (\"end\" int);\nCREATE FUNCTION plots_end() RETURNS integer LANGUAGE SQL\nBEGIN ATOMIC\n    SELECT 1 AS end;\nEND;")
        coordinator.registrationClosed()
        assertEquals(1, plots.await())
        assertEquals(listOf(1), rows("select plots_end()") { it.getInt(1) })
    }

    @Test fun `BEGIN and END inside function bodies and DO blocks run`() {
        val coordinator = session()
        val plots = coordinator.add("plots", "V001__functions.sql" to """
            create function plots_one() returns int language plpgsql as $$ begin return 1; end $$;
            do $$ begin perform plots_one(); end $$;
            create function plots_two() returns int language sql begin atomic select case when true then 2 end; end;
        """.trimIndent())
        coordinator.registrationClosed()
        assertEquals(1, plots.await())
        assertEquals(listOf(2), rows("select plots_two()") { it.getInt(1) })
    }

    @Test fun `requires-if-present on a plugin that another server installed while this one planned fails with restart advice`() {
        val server = interleaved { otherServerApplies("meta", "V001__meta.sql" to "create table meta_table (id int);") }
        val alpha = server.add("alpha", "V001__alpha.sql" to "-- requires-if-present: meta >= 2\ncreate table alpha_table (id int);")
        server.registrationClosed()
        val message = failure(alpha)
        assertTrue(message.contains("meta had no rows when this server planned its migrations"), message)
        assertTrue(message.contains("restart the server"), message)
        assertFalse(exists("alpha_table"))
    }

    @Test fun `a plugin whose startup migration failed still counts as installed for later registrations`() {
        val coordinator = session()
        val broken = coordinator.add("broken", "V001__broken.sql" to "insert into missing_table values (1);")
        coordinator.registrationClosed()
        failure(broken)
        val optional = coordinator.add("optional", "V001__optional.sql" to "-- requires-if-present: broken >= 1\ncreate table optional_table (id int);")
        val message = failure(optional)
        assertTrue(message.contains("broken's migrations failed earlier on this server"), message)
        assertFalse(exists("optional_table"))
        val hard = coordinator.add("hard", "V001__hard.sql" to "-- requires: broken >= 1\ncreate table hard_table (id int);")
        val hardMessage = failure(hard)
        assertTrue(hardMessage.contains("broken's migrations failed earlier on this server"), hardMessage)
        assertFalse(hardMessage.contains("has not registered"), hardMessage)
    }

    private fun sql2(statement: String) = connect(admin!!.database).use { c -> c.createStatement().use { it.execute(statement) } }
}
