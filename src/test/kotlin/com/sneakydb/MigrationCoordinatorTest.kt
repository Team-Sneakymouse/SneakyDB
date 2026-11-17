package com.sneakydb

import com.sneakydb.MigrationScriptsTest.Companion.scripts
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.sql.SQLException
import java.util.concurrent.AbstractExecutorService
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.TimeUnit

class MigrationCoordinatorTest {
    /** Runs every batch inline, so a test sees its outcome as soon as the call returns. */
    class Inline : AbstractExecutorService() {
        var stopped = false
        override fun execute(task: Runnable) { if (stopped) throw java.util.concurrent.RejectedExecutionException(); task.run() }
        override fun shutdown() { stopped = true }
        override fun shutdownNow(): MutableList<Runnable> { stopped = true; return mutableListOf() }
        override fun isShutdown() = stopped
        override fun isTerminated() = stopped
        override fun awaitTermination(timeout: Long, unit: TimeUnit) = true
    }

    /**
     * The version table in memory, with the same under-lock re-checks as the PostgreSQL store; [failing] scripts fail
     * while running with a PostgreSQL-style error, [rejected] ones fail under the lock before they start.
     */
    internal class FakeStore : MigrationStore {
        val rows = sortedMapOf<String, MutableMap<Int, String?>>()
        val log = mutableListOf<String>()
        val failing = mutableSetOf<String>()
        val rejected = mutableSetOf<String>()
        var readFailure: Throwable? = null
        var fillFailure: Throwable? = null
        val modes = mutableListOf<MigrationMode>()

        private fun snapshot() = rows.mapValues { (_, versions) -> versions.map { AppliedVersion(it.key, it.value) } }

        /** Runs once, inside the next read: what happens while a batch is being planned. */
        var duringRead: (() -> Unit)? = null

        override fun readApplied(mode: MigrationMode): Map<String, List<AppliedVersion>> {
            modes.add(mode)
            readFailure?.let { throw it }
            duringRead?.also { duringRead = null }?.invoke()
            return snapshot()
        }

        override fun fillChecksums(plugin: String, scripts: List<MigrationScript>): List<Int> {
            fillFailure?.let { throw it }
            val versions = recheckFill(plugin, scripts, snapshot())
            log.add("fill $plugin $versions")
            versions.forEach { rows.getValue(plugin)[it] = scripts[it - 1].checksum }
            return versions
        }

        override fun apply(step: PlannedStep, lockTimeoutSeconds: Int): Boolean {
            val name = step.toString()
            if (!recheckStep(step, snapshot())) return false
            if (name in rejected) throw MigrationException("expected ${step.plugin} at version 0 before this script, but the database has version 1; restart the server")
            if (name in failing) throw ScriptFailedException(describeFailure(SQLException("ERROR: relation \"missing\" does not exist\n  Position: 15", "42P01")))
            rows.getOrPut(step.plugin) { mutableMapOf() }[step.script.version] = step.script.checksum
            log.add(name)
            return true
        }

        override fun confirmReady(plugin: String, scripts: List<MigrationScript>) {
            recheckReady(plugin, scripts, snapshot())
            log.add("ready $plugin")
        }
    }

    private val store = FakeStore()
    private val info = mutableListOf<String>()
    private val severe = mutableListOf<String>()
    private val ready = CompletableFuture<Void>()

    private fun coordinator(mode: MigrationMode = MigrationMode.APPLY) =
        MigrationCoordinator(MigrationSettings(mode, 10), store, { ready }, info::add, severe::add, Inline())

    private fun MigrationCoordinator.register(plugin: String, vararg bodies: String): CompletableFuture<Int> =
        register(plugin) { scripts(plugin, *bodies.mapIndexed { i, body -> "V${i + 1}__s${i + 1}.sql" to "$body\nselect 1;" }.toTypedArray()) }

    private fun failureOf(future: CompletableFuture<*>): String =
        assertThrows(CompletionException::class.java) { future.join() }.cause!!.message!!

    @Test fun `startup run applies every plugin's scripts in order and completes each future with its version`() {
        ready.complete(null)
        val coordinator = coordinator()
        val beta = coordinator.register("beta", "-- requires: alpha >= 1", "-- requires: alpha >= 2")
        val alpha = coordinator.register("alpha", "", "-- requires: beta >= 1", "-- requires: beta >= 2")
        assertFalse(alpha.isDone, "registration alone runs nothing")
        assertTrue(store.log.isEmpty())
        coordinator.registrationClosed()
        assertEquals(listOf("alpha:V1__s1.sql", "beta:V1__s1.sql", "alpha:V2__s2.sql", "beta:V2__s2.sql", "ready beta", "alpha:V3__s3.sql", "ready alpha"), store.log)
        assertEquals(3, alpha.join())
        assertEquals(2, beta.join())
        assertEquals(StartupOutcome.CLEAN, coordinator.startup.join())
        assertEquals(6, info.size, "one line per script and a summary")
        assertTrue(info[0].startsWith("Applied migration alpha:V1__s1.sql in "))
        assertTrue(info.last().startsWith("Migrations (startup, apply mode): 5 scripts applied, 2 of 2 plugins ready in "), info.last())
        assertTrue(severe.isEmpty())
    }

    @Test fun `nothing pending completes at once and a restart applies nothing`() {
        ready.complete(null)
        store.rows["alpha"] = mutableMapOf()
        val first = coordinator()
        val version = first.register("alpha", "", "")
        first.registrationClosed()
        assertEquals(2, version.join())
        store.log.clear()
        val second = coordinator()
        val again = second.register("alpha", "", "")
        second.registrationClosed()
        assertEquals(2, again.join())
        assertEquals(listOf("ready alpha"), store.log, "nothing applied, only the check under the lock")
    }

    @Test fun `no registrations opens the startup result as soon as registration closes, even before readiness`() {
        val coordinator = coordinator()
        coordinator.registrationClosed()
        assertEquals(StartupOutcome.CLEAN, coordinator.startup.join())
        assertTrue(store.modes.isEmpty())
    }

    @Test fun `a script that fails while running stops the run and fails every plugin not completed yet`() {
        ready.complete(null)
        store.rows["done"] = mutableMapOf(1 to null)
        store.failing.add("alpha:V2__s2.sql")
        val coordinator = coordinator()
        val done = coordinator.register("done", "") // nothing pending: completes before any script runs
        val alpha = coordinator.register("alpha", "", "", "")
        val beta = coordinator.register("beta", "-- requires: alpha >= 2")
        val gamma = coordinator.register("gamma", "", "")
        coordinator.registrationClosed()
        assertEquals(listOf("fill done [1]", "ready done", "alpha:V1__s1.sql"), store.log, "gamma's independent scripts don't run either")
        assertEquals(1, done.join(), "a plugin completed before the failure stays completed")
        assertEquals("Migrations for alpha failed at V2__s2.sql: SQL state 42P01: ERROR: relation \"missing\" does not exist; Position: 15", failureOf(alpha))
        for ((plugin, future) in listOf("beta" to beta, "gamma" to gamma)) {
            assertEquals("Migrations for $plugin failed: not completed, because alpha:V2__s2.sql failed while running and SneakyDB ran no further script",
                failureOf(future))
        }
        assertEquals(StartupOutcome.STOPPED, coordinator.startup.join())
        assertEquals(2, severe.size, "the failed script's line and one summary: $severe")
        assertTrue(severe[1].startsWith("Migrations (startup, apply mode) stopped: alpha:V2__s2.sql failed while running, so no further " +
            "script ran; 1 scripts applied, 1 of 4 plugins ready; failed: alpha, beta, gamma. The server stops"), severe[1])
        assertTrue(info.none { it.startsWith("Migrations (") }, "the summary is the severe line")
    }

    @Test fun `a script rejected under the lock before it started fails only its plugin and its dependents`() {
        ready.complete(null)
        store.rejected.add("alpha:V2__s2.sql")
        val coordinator = coordinator()
        val alpha = coordinator.register("alpha", "", "", "")
        val beta = coordinator.register("beta", "-- requires: alpha >= 2")
        val gamma = coordinator.register("gamma", "", "")
        coordinator.registrationClosed()
        assertEquals(listOf("alpha:V1__s1.sql", "gamma:V1__s1.sql", "gamma:V2__s2.sql", "ready gamma"), store.log)
        assertTrue(failureOf(alpha).endsWith("restart the server"))
        assertEquals("Migrations for beta failed at V1__s1.sql: not run because it requires alpha:V2__s2.sql, which failed", failureOf(beta))
        assertEquals(2, gamma.join())
        assertEquals(StartupOutcome.PLUGINS_FAILED, coordinator.startup.join())
        assertEquals(2, severe.size, "one severe line per failed plugin")
        assertTrue(info.last().contains("1 of 3 plugins ready; failed: alpha, beta"), info.last())
    }

    @Test fun `a script failing while running wins over failures caught before the run`() {
        ready.complete(null)
        store.failing.add("gamma:V1__s1.sql")
        val coordinator = coordinator()
        val alpha = coordinator.register("alpha", "-- requires: beta >= 1")
        val beta = coordinator.register("beta", "-- requires: alpha >= 1")
        val gamma = coordinator.register("gamma", "")
        coordinator.registrationClosed()
        assertTrue(failureOf(alpha).contains("form a cycle"))
        assertTrue(failureOf(beta).contains("form a cycle"))
        assertTrue(failureOf(gamma).contains("SQL state 42P01"))
        assertEquals(StartupOutcome.STOPPED, coordinator.startup.join())
    }

    @Test fun `after the startup run stopped, what registered during it and later fails and runs nothing`() {
        ready.complete(null)
        store.failing.add("alpha:V1__s1.sql")
        val executor = Queued()
        val coordinator = MigrationCoordinator(MigrationSettings(MigrationMode.APPLY, 10), store, { ready }, info::add, severe::add, executor)
        val alpha = coordinator.register("alpha", "")
        lateinit var during: CompletableFuture<Int>
        store.duringRead = { during = coordinator.register("during", "") } // registers while the startup run is going
        coordinator.registrationClosed()
        executor.runAll() // the startup run, then the late task that was queued for "during"
        assertTrue(failureOf(alpha).contains("SQL state 42P01"))
        assertEquals(StartupOutcome.STOPPED, coordinator.startup.join())
        assertTrue(failureOf(during).startsWith("Migrations stopped: a script failed while running during startup"), failureOf(during))
        val after = coordinator.register("after", "")
        executor.runAll()
        assertTrue(failureOf(after).startsWith("Migrations stopped: a script failed while running during startup"), failureOf(after))
        assertTrue(store.log.isEmpty(), "no script of a late plugin ran: ${store.log}")
        assertTrue(store.rows.isEmpty(), store.rows.toString())
    }

    @Test fun `a late batch stops at a script that fails while running, without touching the startup outcome`() {
        ready.complete(null)
        val executor = Queued()
        val coordinator = MigrationCoordinator(MigrationSettings(MigrationMode.APPLY, 10), store, { ready }, info::add, severe::add, executor)
        coordinator.registrationClosed()
        executor.runAll()
        assertEquals(StartupOutcome.CLEAN, coordinator.startup.join())
        store.failing.add("la:V1__s1.sql")
        val la = coordinator.register("la", "")
        val lb = coordinator.register("lb", "")
        executor.runAll()
        assertTrue(failureOf(la).contains("SQL state 42P01"))
        assertTrue(failureOf(lb).contains("not completed, because la:V1__s1.sql failed while running"))
        assertTrue(store.log.isEmpty(), store.log.toString())
        assertTrue(severe.last().startsWith("Migrations (late registration of la, lb, apply mode) stopped: la:V1__s1.sql failed while running"), severe.last())
        assertFalse(severe.last().contains("server stops"), severe.last())
        assertEquals(StartupOutcome.CLEAN, coordinator.startup.join())
    }

    @Test fun `closing registration outside startup runs what registered as a late batch and reports a clean startup`() {
        ready.complete(null)
        store.failing.add("alpha:V1__s1.sql")
        val coordinator = coordinator()
        val alpha = coordinator.register("alpha", "")
        coordinator.registrationClosed(startupRun = false)
        assertEquals(StartupOutcome.CLEAN, coordinator.startup.join())
        assertTrue(failureOf(alpha).contains("SQL state 42P01"))
        assertTrue(severe.last().startsWith("Migrations (late registration of alpha, apply mode) stopped"), severe.last())
        coordinator.registrationClosed(startupRun = true)
        assertEquals(StartupOutcome.CLEAN, coordinator.startup.join(), "only the first close counts")
    }

    @Test fun `awaiting the startup run returns its outcome, or null once SneakyDB shut down`() {
        val coordinator = MigrationCoordinator(MigrationSettings.DEFAULTS, store, { ready }, info::add, severe::add)
        coordinator.register("alpha", "")
        coordinator.registrationClosed()
        val waited = CompletableFuture.supplyAsync { awaitStartup(coordinator.startup) }
        Thread.sleep(100)
        assertFalse(waited.isDone, "readiness is pending: the run hasn't ended")
        coordinator.close()
        assertNull(waited.get(5, TimeUnit.SECONDS))
        ready.completeExceptionally(IllegalStateException("SneakyDB has shut down")) // frees the migration thread
        val clean = coordinator()
        clean.registrationClosed()
        assertEquals(StartupOutcome.CLEAN, awaitStartup(clean.startup))
    }

    @Test fun `plan failures fail only their own group before anything runs`() {
        ready.complete(null)
        val coordinator = coordinator()
        val alpha = coordinator.register("alpha", "-- requires: beta >= 1")
        val beta = coordinator.register("beta", "-- requires: alpha >= 1")
        val broken = coordinator.register("broken") { throw MigrationException("broken: no migration scripts found") }
        val gamma = coordinator.register("gamma", "")
        coordinator.registrationClosed()
        assertEquals(listOf("gamma:V1__s1.sql", "ready gamma"), store.log)
        assertTrue(failureOf(alpha).contains("form a cycle"))
        assertTrue(failureOf(beta).contains("form a cycle"))
        assertEquals("Migrations for broken failed: broken: no migration scripts found", failureOf(broken))
        assertEquals(1, gamma.join())
        assertEquals(3, severe.size)
    }

    @Test fun `unexpected discovery errors name only their class`() {
        ready.complete(null)
        val coordinator = coordinator()
        val future = coordinator.register("alpha") { throw java.io.IOException("C:\\secret\\path") }
        coordinator.registrationClosed()
        assertEquals("Migrations for alpha failed: alpha: could not read its migration scripts (unexpected IOException)", failureOf(future))
    }

    @Test fun `database initialization failure fails every registered plugin`() {
        val coordinator = coordinator()
        val alpha = coordinator.register("alpha", "")
        val beta = coordinator.register("beta", "")
        val message = "SneakyDB initialization failed during connection acquisition; check adapter configuration and database access"
        ready.completeExceptionally(IllegalStateException(message))
        coordinator.registrationClosed()
        assertEquals("Migrations for alpha failed: $message", failureOf(alpha))
        assertEquals("Migrations for beta failed: $message", failureOf(beta))
        assertEquals(StartupOutcome.PLUGINS_FAILED, coordinator.startup.join())
        assertTrue(store.modes.isEmpty())
    }

    @Test fun `a version table problem fails every plugin of the batch with the operator's instructions`() {
        ready.complete(null)
        store.readFailure = MigrationException("the schema sneakydb does not exist; as the database owner run: create schema sneakydb; ...")
        val coordinator = coordinator()
        val alpha = coordinator.register("alpha", "")
        coordinator.registrationClosed()
        assertTrue(failureOf(alpha).startsWith("Migrations for alpha failed: the schema sneakydb does not exist"))
        assertEquals(StartupOutcome.PLUGINS_FAILED, coordinator.startup.join())
    }

    @Test fun `connection failures don't repeat driver messages`() {
        ready.complete(null)
        store.readFailure = SQLException("password authentication failed for user secret", "08001")
        val coordinator = coordinator()
        val alpha = coordinator.register("alpha", "")
        coordinator.registrationClosed()
        assertEquals("Migrations for alpha failed: could not use a database connection (SQLException, SQL state 08001)", failureOf(alpha))
    }

    @Test fun `late registrations run at once as their own batch and never change the startup result`() {
        ready.complete(null)
        val coordinator = coordinator()
        coordinator.registrationClosed()
        assertEquals(StartupOutcome.CLEAN, coordinator.startup.join())
        val late = coordinator.register("late", "", "")
        assertEquals(2, late.join())
        assertEquals(listOf("late:V1__s1.sql", "late:V2__s2.sql", "ready late"), store.log)
        assertTrue(info.last().startsWith("Migrations (late registration of late, apply mode): 2 scripts applied"), info.last())

        store.failing.add("failing:V1__s1.sql")
        val failing = coordinator.register("failing", "")
        assertTrue(failureOf(failing).startsWith("Migrations for failing failed at V1__s1.sql"))
        assertEquals(StartupOutcome.CLEAN, coordinator.startup.join())
    }

    /** Queues batches until [runAll], like the migration thread while it is busy with an earlier batch. */
    class Queued : AbstractExecutorService() {
        private val tasks = ArrayDeque<Runnable>()
        override fun execute(task: Runnable) { tasks.add(task) }
        fun runAll() { while (tasks.isNotEmpty()) tasks.removeFirst().run() }
        override fun shutdown() {}
        override fun shutdownNow(): MutableList<Runnable> = mutableListOf()
        override fun isShutdown() = false
        override fun isTerminated() = false
        override fun awaitTermination(timeout: Long, unit: TimeUnit) = true
    }

    @Test fun `late registrations queued at the same time are planned together as one batch`() {
        ready.complete(null)
        val executor = Queued()
        val coordinator = MigrationCoordinator(MigrationSettings(MigrationMode.APPLY, 10), store, { ready }, info::add, severe::add, executor)
        coordinator.registrationClosed()
        val lp = coordinator.register("lp", "-- requires-if-present: lq >= 1")
        val lq = coordinator.register("lq", "")
        executor.runAll()
        assertEquals(1, lp.join())
        assertEquals(1, lq.join())
        assertEquals(listOf("lq:V1__s1.sql", "ready lq", "lp:V1__s1.sql", "ready lp"), store.log)
        assertTrue(info.last().startsWith("Migrations (late registration of lp, lq, apply mode): 2 scripts applied"), info.last())
        assertTrue(severe.isEmpty(), severe.toString())
    }

    @Test fun `a plugin that registers while a late batch is planned is not taken as having its final version`() {
        ready.complete(null)
        val executor = Queued()
        val coordinator = MigrationCoordinator(MigrationSettings(MigrationMode.APPLY, 10), store, { ready }, info::add, severe::add, executor)
        coordinator.registrationClosed()
        var lq: CompletableFuture<Int>? = null
        store.duringRead = { lq = coordinator.register("lq", "") }
        val lp = coordinator.register("lp", "-- requires-if-present: lq >= 1")
        executor.runAll()
        val message = failureOf(lp)
        assertTrue(message.contains("lq registered after this plugin's migrations were planned"), message)
        assertFalse(message.contains("goes no higher"), message)
        assertEquals(1, lq!!.join(), "lq's own batch runs right after")
    }

    @Test fun `a re-enabled plugin may register again once its previous registration finished`() {
        ready.complete(null)
        val coordinator = coordinator()
        val first = coordinator.register("alpha", "")
        val duplicate = coordinator.register("alpha", "")
        assertTrue(failureOf(duplicate).contains("already registered and not finished"))
        coordinator.registrationClosed()
        assertEquals(1, first.join())
        assertEquals(2, coordinator.register("alpha", "", "").join(), "re-enabled with a newer build")
        assertEquals(listOf("alpha:V1__s1.sql", "ready alpha", "alpha:V2__s2.sql", "ready alpha"), store.log)
    }

    @Test fun `plugin ids must be lower-case identifiers`() {
        val coordinator = coordinator()
        for (bad in listOf("SneakyPlots", "1plots", "", "sneaky-plots", "a".repeat(64))) {
            val error = assertThrows(CompletionException::class.java) { coordinator.register(bad, "").join() }
            assertInstanceOf(IllegalArgumentException::class.java, error.cause, bad)
        }
        assertFalse(coordinator.register("a".repeat(63), "").isDone)
    }

    @Test fun `verify mode never writes and fails plugins with pending scripts`() {
        ready.complete(null)
        store.rows["alpha"] = mutableMapOf(1 to null)
        store.rows["beta"] = mutableMapOf(1 to null)
        val coordinator = coordinator(MigrationMode.VERIFY)
        val alpha = coordinator.register("alpha", "")
        val beta = coordinator.register("beta", "", "")
        coordinator.registrationClosed()
        assertEquals(1, alpha.join())
        assertTrue(failureOf(beta).contains("verify mode"))
        assertEquals(listOf(MigrationMode.VERIFY), store.modes)
        assertEquals(listOf("ready alpha"), store.log, "no checksum fill, no script")
        assertNull(store.rows.getValue("alpha")[1])
    }

    @Test fun `apply mode fills operator rows before running the next scripts`() {
        ready.complete(null)
        store.rows["alpha"] = mutableMapOf(1 to null)
        val coordinator = coordinator()
        val alpha = coordinator.register("alpha", "", "")
        coordinator.registrationClosed()
        assertEquals(2, alpha.join())
        assertEquals(listOf("fill alpha [1]", "alpha:V2__s2.sql", "ready alpha"), store.log)
        assertNotNull(store.rows.getValue("alpha")[1])
    }

    @Test fun `a plugin with nothing pending completes only after its checksum fill-in succeeded`() {
        ready.complete(null)
        store.rows["alpha"] = mutableMapOf(1 to null)
        store.fillFailure = SQLException("ERROR: permission denied for table schema_migrations", "42501")
        val coordinator = coordinator()
        val alpha = coordinator.register("alpha", "")
        coordinator.registrationClosed()
        assertEquals("Migrations for alpha failed: SQL state 42501: ERROR: permission denied for table schema_migrations", failureOf(alpha))
        assertEquals(StartupOutcome.PLUGINS_FAILED, coordinator.startup.join())
        assertTrue(store.log.isEmpty(), "never declared ready")
    }

    @Test fun `a plugin whose rows stopped matching its jar under the lock fails instead of completing`() {
        ready.complete(null)
        store.rows["alpha"] = mutableMapOf()
        // Another server applies alpha 1 and 2 after this server read the table: this server's jar only has 1.
        val racing = object : MigrationStore by store {
            override fun readApplied(mode: MigrationMode) = store.readApplied(mode).also {
                store.rows["alpha"] = mutableMapOf(1 to null, 2 to null)
            }
        }
        val coordinator = MigrationCoordinator(MigrationSettings.DEFAULTS, racing, { ready }, info::add, severe::add, Inline())
        val alpha = coordinator.register("alpha", "")
        coordinator.registrationClosed()
        assertTrue(failureOf(alpha).contains("the database is newer than this build: it is at version 2, this build's scripts go up to 1"))
        assertEquals(StartupOutcome.PLUGINS_FAILED, coordinator.startup.join())
    }

    @Test fun `a script another server applied meanwhile is skipped and counted`() {
        ready.complete(null)
        // Another server applies alpha 1 after this server read the table.
        val racing = object : MigrationStore by store {
            override fun readApplied(mode: MigrationMode) = store.readApplied(mode).also { store.rows["alpha"] = mutableMapOf(1 to null) }
        }
        val coordinator = MigrationCoordinator(MigrationSettings.DEFAULTS, racing, { ready }, info::add, severe::add, Inline())
        val alpha = coordinator.register("alpha", "")
        coordinator.registrationClosed()
        assertEquals(1, alpha.join())
        assertEquals("Migration alpha:V1__s1.sql was already applied by another server.", info[0])
        assertTrue(info.last().contains("0 scripts applied (1 already applied by another server)"), info.last())
    }

    @Test fun `close fails pending and later registrations without severe lines`() {
        val coordinator = coordinator()
        val pending = coordinator.register("alpha", "")
        coordinator.close()
        assertEquals("SneakyDB has shut down", failureOf(pending))
        assertEquals("SneakyDB has shut down", failureOf(coordinator.register("beta", "")))
        assertThrows(CompletionException::class.java) { coordinator.startup.join() }
        coordinator.registrationClosed()
        assertTrue(severe.isEmpty())
    }

    @Test fun `settings read mode, bounds and message with safe fallbacks`() {
        val warnings = mutableListOf<String>()
        fun read(vararg values: Pair<String, Any?>) = MigrationSettings.read({ values.toMap()[it] }, warnings::add)
        val defaults = read()
        assertEquals(MigrationMode.APPLY, defaults.mode)
        assertEquals(10, defaults.lockTimeoutSeconds)
        assertEquals(MigrationMode.VERIFY, read("mode" to " Verify ").mode)
        assertTrue(warnings.isEmpty())
        assertEquals(MigrationMode.VERIFY, read("mode" to "aply").mode, "a typo never writes")
        assertEquals(1, warnings.size)
        assertEquals(1, read("lock-timeout-seconds" to 0).lockTimeoutSeconds)
        assertEquals(300, read("lock-timeout-seconds" to 9999).lockTimeoutSeconds)
        assertEquals(10, read("lock-timeout-seconds" to "5").lockTimeoutSeconds)
        val old = read("login-message" to "Back soon", "mode" to "verify")
        assertEquals(MigrationMode.VERIFY, old.mode, "a config from before still reads; its login-message is ignored")
        assertEquals(1, warnings.size)
    }
}
