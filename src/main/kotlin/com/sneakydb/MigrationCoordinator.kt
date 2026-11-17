package com.sneakydb

import java.util.concurrent.CancellationException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

// Plugin-owned migrations: registration, the startup run and late batches. No platform types here.

/** The `migrations:` config section, read with the same rules on Paper and Velocity. */
internal class MigrationSettings(val mode: MigrationMode, val lockTimeoutSeconds: Int) {
    companion object {
        val DEFAULTS = MigrationSettings(MigrationMode.APPLY, 10)

        /**
         * [value] returns the raw value under `migrations.<key>`, or null when absent. A missing mode means apply; an
         * unknown one means verify, with a warning, so a typo never lets a server write to the database. Keys that
         * older versions read (`login-message`) are ignored.
         */
        fun read(value: (String) -> Any?, warn: (String) -> Unit): MigrationSettings {
            val rawMode = value("mode")?.toString()?.trim()?.lowercase()
            val mode = when (rawMode) {
                null, "apply" -> MigrationMode.APPLY
                "verify" -> MigrationMode.VERIFY
                else -> MigrationMode.VERIFY.also { warn("migrations.mode must be apply or verify; running in verify mode") }
            }
            return MigrationSettings(mode, ((value("lock-timeout-seconds") as? Number)?.toInt() ?: 10).coerceIn(1, 300))
        }
    }
}

/** A player-facing message from config: nonblank, one line, at most 512 characters, otherwise [default]. */
internal fun playerMessage(text: String?, default: String): String =
    text?.takeIf { it.isNotBlank() && it.length <= 512 && it.none(Char::isISOControl) } ?: default

/** Database side of migrations. Calls block; the coordinator makes them on its own thread, never on a worker. */
internal interface MigrationStore {
    /**
     * Every row of the version table, by plugin. In apply mode creates the table when it is missing; in verify mode a
     * missing table reads as empty. Throws [MigrationException] when the `sneakydb` schema is missing or unusable.
     */
    fun readApplied(mode: MigrationMode): Map<String, List<AppliedVersion>>

    /**
     * Under the migration lock: re-checks [plugin]'s rows against [scripts] ([recheckFill]) and records the jar's
     * checksum on its rows that have none (operator rows). Returns their versions.
     */
    fun fillChecksums(plugin: String, scripts: List<MigrationScript>): List<Int>

    /**
     * Applies [step] and records its version in one transaction, under the migration lock and after re-checking what is
     * applied ([recheckStep]). Returns false when another server applied it in the meantime. Throws
     * [ScriptFailedException] for a failure once the script had started; any other exception means it never started.
     */
    fun apply(step: PlannedStep, lockTimeoutSeconds: Int): Boolean

    /** Under the migration lock, writing nothing: throws unless [plugin]'s rows are exactly [scripts] ([recheckReady]). */
    fun confirmReady(plugin: String, scripts: List<MigrationScript>)
}

/** How the startup run ended. The platform decides from it whether its startup continues. */
internal enum class StartupOutcome {
    /** No plugin failed, or there was nothing to run. */
    CLEAN,

    /** Some plugins failed before any of their scripts ran, so the database is unchanged by them; the others migrated. */
    PLUGINS_FAILED,

    /** A script failed while running: no further script ran, and every plugin not completed by then failed. */
    STOPPED,
}

/**
 * Blocks the calling platform thread (Paper's server thread, Velocity's initialisation) until [startup] completes, and
 * returns its outcome; null when SneakyDB shut down first. Interrupts don't end the wait; they are kept for the caller.
 */
internal fun awaitStartup(startup: CompletableFuture<StartupOutcome>): StartupOutcome? {
    var interrupted = false
    try {
        while (true) {
            try {
                return startup.get()
            } catch (_: InterruptedException) {
                interrupted = true
            } catch (_: ExecutionException) {
                return null
            } catch (_: CancellationException) {
                return null
            }
        }
    } finally {
        if (interrupted) Thread.currentThread().interrupt()
    }
}

internal fun migrationThread(): ExecutorService = ThreadPoolExecutor(1, 1, 30, TimeUnit.SECONDS, LinkedBlockingQueue()) { task ->
    Thread(task, "SneakyDB-Migrations").apply {
        isDaemon = true
        contextClassLoader = DatabaseSession::class.java.classLoader
    }
}.apply { allowCoreThreadTimeOut(true) }

/**
 * One enable generation's migrations. [register] only records a plugin; [registrationClosed] starts the run of
 * everything registered so far: the startup run (Paper: `ServerLoadEvent` at startup; Velocity: the end of proxy
 * initialisation), whose outcome [startup] reports and which the platform waits for. Later registrations wait in a
 * queue; when the executor gets to them, every registration queued by then forms one late batch. Batches run one after
 * another on [executor]. A script that fails while running stops its batch. [startup] never completes normally after
 * [close].
 */
internal class MigrationCoordinator(
    private val settings: MigrationSettings,
    private val store: MigrationStore,
    private val ready: () -> CompletableFuture<Void>,
    private val info: (String) -> Unit,
    private val severe: (String) -> Unit,
    private val executor: ExecutorService = migrationThread(),
) {
    private class Registration(val plugin: String, val scripts: () -> List<MigrationScript>, val result: CompletableFuture<Int>)

    private val lock = Any()
    private var registering = true
    private var stopped = false
    private val startupBatch = mutableListOf<Registration>()
    /** Late registrations not yet taken into a batch, and whether a task to take them is already submitted. */
    private val lateQueue = mutableListOf<Registration>()
    private var lateTaskSubmitted = false
    /** Registrations whose future is not complete yet, by plugin. */
    private val pending = HashMap<String, Registration>()
    /** Every plugin that registered in this enable generation, with the reason its latest registration failed, if it did. */
    private val generation = HashMap<String, String?>()
    val startup = CompletableFuture<StartupOutcome>()

    fun register(plugin: String, anchor: Class<*>): CompletableFuture<Int> = register(plugin) { discoverMigrationScripts(plugin, anchor) }

    internal fun register(plugin: String, scripts: () -> List<MigrationScript>): CompletableFuture<Int> {
        val result = CompletableFuture<Int>()
        if (!PLUGIN_ID.matches(plugin)) {
            result.completeExceptionally(IllegalArgumentException(
                "Migration plugin id '$plugin' must match [a-z][a-z0-9_]{0,62}; pass the plugin's lower-case IDENTIFIER"))
            return result
        }
        val registration = Registration(plugin, scripts, result)
        val submitLateTask = synchronized(lock) {
            if (stopped) {
                result.completeExceptionally(IllegalStateException("SneakyDB has shut down"))
                return result
            }
            if (pending.containsKey(plugin)) {
                result.completeExceptionally(IllegalStateException(
                    "Migrations for $plugin are already registered and not finished; call migrate() once per enable"))
                return result
            }
            pending[plugin] = registration
            generation.putIfAbsent(plugin, null)
            // Attached before the lock is released: a task already submitted may take the registration right after.
            result.whenComplete { _, failure ->
                synchronized(lock) {
                    pending.remove(plugin, registration)
                    generation[plugin] = failure?.let { describeFailure(it) }
                }
            }
            if (registering) {
                startupBatch.add(registration)
                false
            } else {
                lateQueue.add(registration)
                !lateTaskSubmitted.also { lateTaskSubmitted = true }
            }
        }
        if (submitLateTask) submit(startupRun = false) {
            synchronized(lock) {
                lateTaskSubmitted = false
                lateQueue.toList().also { lateQueue.clear() }
            }
        }
        return result
    }

    /**
     * Every plugin has had its chance to register: run what is registered. With [startupRun] that is the startup run,
     * reported through [startup]; otherwise (SneakyDB enabled after the server started) it runs as a late batch and
     * [startup] reports [StartupOutcome.CLEAN]. Only the first call does anything.
     */
    fun registrationClosed(startupRun: Boolean = true) {
        val batch = synchronized(lock) {
            if (!registering || stopped) return
            registering = false
            startupBatch.toList().also { startupBatch.clear() }
        }
        if (batch.isEmpty() || !startupRun) startup.complete(StartupOutcome.CLEAN)
        if (batch.isNotEmpty()) submit(startupRun) { batch }
    }

    fun close() {
        val outstanding = synchronized(lock) {
            stopped = true
            pending.values.toList().also { startupBatch.clear(); lateQueue.clear() }
        }
        val error = IllegalStateException("SneakyDB has shut down")
        outstanding.forEach { it.result.completeExceptionally(error) }
        startup.completeExceptionally(error)
        executor.shutdownNow()
    }

    private val isStopped get() = synchronized(lock) { stopped }

    /** Runs the batch that [take] returns on the executor; late batches take what is queued when they start. */
    private fun submit(startupRun: Boolean, take: () -> List<Registration>) {
        try {
            executor.execute {
                val batch = take()
                if (batch.isEmpty()) return@execute
                val label = if (startupRun) "startup" else "late registration of ${batch.joinToString { it.plugin }}"
                // A run that broke without saying how far it got is treated like a script that failed while running.
                var outcome = StartupOutcome.STOPPED
                try {
                    outcome = runBatch(batch, label, startupRun)
                } catch (error: Throwable) {
                    if (!isStopped) severe("Migrations ($label) stopped: ${describeFailure(error)}")
                } finally {
                    // Never leave a future pending, even if the run itself broke.
                    val error = MigrationException("Migrations stopped before they finished; see the server log")
                    batch.forEach { if (!it.result.isDone) { outcome = StartupOutcome.STOPPED; it.result.completeExceptionally(error) } }
                    if (startupRun) startup.complete(outcome)
                }
            }
        } catch (_: RejectedExecutionException) {
            val error = IllegalStateException("SneakyDB has shut down")
            take().forEach { it.result.completeExceptionally(error) }
            startup.completeExceptionally(error)
        }
    }

    /** Runs one batch until it ends or a script fails while running. */
    private fun runBatch(batch: List<Registration>, label: String, startupRun: Boolean): StartupOutcome {
        val started = System.nanoTime()
        val run = BatchRun(batch.associateBy { it.plugin })
        try {
            ready().join()
        } catch (error: Throwable) {
            batch.forEach { run.fail(it.plugin, null, describeFailure(error)) }
            return run.finish(label, started, startupRun)
        }
        val discovered = batch.associate { registration ->
            registration.plugin to runCatching { registration.scripts() }.recoverCatching { error ->
                if (error is MigrationException) throw error
                throw MigrationException("${registration.plugin}: could not read its migration scripts (${describeFailure(error)})")
            }
        }
        val applied = try {
            store.readApplied(settings.mode)
        } catch (error: Throwable) {
            batch.forEach { run.fail(it.plugin, null, describeFailure(error)) }
            return run.finish(label, started, startupRun)
        }
        // Plugins of other batches of this generation count as present, failed ones included. Those still pending
        // registered after this batch was put together: their version is not final until their own batch has run.
        val (earlier, waiting) = synchronized(lock) {
            generation.filterKeys { it !in discovered } to pending.keys.filterTo(HashSet()) { it !in discovered }
        }
        val plan = planMigrations(discovered, applied, settings.mode, earlier, waiting)
        plan.failures.forEach { (plugin, failure) -> run.fail(plugin, failure.script, failure.reason) }

        for (plugin in plan.checksumFills) {
            try {
                val filled = store.fillChecksums(plugin, plan.scripts.getValue(plugin))
                if (filled.isNotEmpty()) info("Recorded checksums for $plugin's operator-recorded versions $filled.")
            } catch (error: Throwable) {
                run.fail(plugin, null, describeFailure(error))
                plan.steps.firstOrNull { it.plugin == plugin }?.let { run.skipFrom(it.key, plan, "$plugin's checksum update") }
            }
        }
        // A plugin is declared ready only after its checksum fill-in and a check of its rows under the lock.
        val remaining = plan.steps.groupingBy { it.plugin }.eachCount().toMutableMap()
        plan.scripts.keys.filter { it !in remaining }.forEach { run.ready(it, plan) }
        for (step in plan.steps) {
            if (isStopped) break
            if (step.key in run.skipped || step.plugin in run.failed) continue
            val begun = System.nanoTime()
            try {
                if (store.apply(step, settings.lockTimeoutSeconds)) {
                    run.applied++
                    info("Applied migration ${step.plugin}:${step.script.fileName} in ${(System.nanoTime() - begun) / 1_000_000} ms.")
                } else {
                    run.skippedByOthers++
                    info("Migration ${step.plugin}:${step.script.fileName} was already applied by another server.")
                }
            } catch (error: Throwable) {
                if (error is ScriptFailedException && !isStopped) {
                    // Keep errors from compounding: nothing further of this batch runs.
                    run.halt(step, describeFailure(error))
                    break
                }
                run.fail(step.plugin, step.script.fileName, describeFailure(error))
                run.skipFrom(step.key, plan, "${step.plugin}:${step.script.fileName}")
                continue
            }
            val left = remaining.merge(step.plugin, -1, Int::plus)!!
            if (left == 0) run.ready(step.plugin, plan)
        }
        return run.finish(label, started, startupRun)
    }

    /** Outcome bookkeeping of one batch. */
    private inner class BatchRun(private val registrations: Map<String, Registration>) {
        val failed = LinkedHashSet<String>()
        val skipped = HashSet<StepKey>()
        private val completed = HashSet<String>()
        /** The script that failed while running and stopped the batch. */
        private var haltedAt: PlannedStep? = null
        var applied = 0
        var skippedByOthers = 0

        /** Completes [plugin] with its schema version once its rows match its jar under the lock; fails it otherwise. */
        fun ready(plugin: String, plan: MigrationPlan) {
            if (plugin in failed) return
            val scripts = plan.scripts.getValue(plugin)
            try {
                store.confirmReady(plugin, scripts)
            } catch (error: Throwable) {
                fail(plugin, null, describeFailure(error))
                return
            }
            completed.add(plugin)
            registrations[plugin]?.result?.complete(scripts.size)
        }

        fun fail(plugin: String, script: String?, reason: String, log: Boolean = true) {
            if (!failed.add(plugin)) return
            val registration = registrations[plugin] ?: return
            if (isStopped) {
                registration.result.completeExceptionally(IllegalStateException("SneakyDB has shut down"))
                return
            }
            val message = "Migrations for $plugin failed${script?.let { " at $it" } ?: ""}: $reason"
            if (log) severe(message)
            registration.result.completeExceptionally(MigrationException(message))
        }

        /** [step] failed while running: fails its plugin and every plugin of the batch not completed yet. */
        fun halt(step: PlannedStep, reason: String) {
            fail(step.plugin, step.script.fileName, reason)
            haltedAt = step
            for (plugin in registrations.keys.sorted()) {
                if (plugin in completed) continue
                fail(plugin, null, "not completed, because $step failed while running and SneakyDB ran no further script", log = false)
            }
        }

        /** Skips [failedStep], everything after it in its plugin and every step that depends on those. */
        fun skipFrom(failedStep: StepKey, plan: MigrationPlan, cause: String) {
            val queue = ArrayDeque(listOf(failedStep))
            while (queue.isNotEmpty()) {
                val step = queue.removeFirst()
                if (!skipped.add(step)) continue
                if (step.plugin != failedStep.plugin) {
                    val name = plan.steps.first { it.key == step }.script.fileName
                    fail(step.plugin, name, "not run because it requires $cause, which failed")
                }
                queue.addAll(plan.dependents[step].orEmpty())
            }
        }

        fun finish(label: String, started: Long, startupRun: Boolean): StartupOutcome {
            val millis = (System.nanoTime() - started) / 1_000_000
            val others = if (skippedByOthers > 0) " ($skippedByOthers already applied by another server)" else ""
            val counts = "$applied scripts applied$others, ${completed.size} of ${registrations.size} plugins ready"
            val halted = haltedAt
            if (!isStopped) {
                val mode = "$label, ${settings.mode.name.lowercase()} mode"
                if (halted != null) {
                    severe("Migrations ($mode) stopped: $halted failed while running, so no further script ran; $counts; " +
                        "failed: ${failed.joinToString()}" + (if (startupRun) ". The server stops so the error can't compound; " +
                        "fix the cause logged above and start it again (each start retries the script)" else "") + " (in $millis ms).")
                } else {
                    val outcome = if (failed.isEmpty()) "" else "; failed: ${failed.joinToString()}"
                    info("Migrations ($mode): $counts$outcome in $millis ms.")
                }
            }
            return when {
                halted != null -> StartupOutcome.STOPPED
                failed.isEmpty() -> StartupOutcome.CLEAN
                else -> StartupOutcome.PLUGINS_FAILED
            }
        }
    }
}

/**
 * A one-line reason for logs and futures. PostgreSQL's SQL state and message are kept for failed scripts; connection
 * failures name only their SQL state, and unknown errors only their class, so nothing credential-bearing is repeated.
 */
internal fun describeFailure(error: Throwable): String {
    var cause = error
    while (cause is CompletionException && cause.cause != null) cause = cause.cause!!
    return when {
        cause is MigrationException -> cause.message!!
        cause is java.sql.SQLTransientConnectionException || cause is java.sql.SQLNonTransientConnectionException ||
            (cause is java.sql.SQLException && cause.sqlState?.startsWith("08") == true) ->
            "could not use a database connection (${cause.javaClass.simpleName}, SQL state ${(cause as java.sql.SQLException).sqlState})"
        cause is java.sql.SQLException ->
            "SQL state ${cause.sqlState}: ${cause.message?.lineSequence()?.map { it.trim() }?.filter { it.isNotEmpty() }?.joinToString("; ")}"
        cause is IllegalStateException && cause.message?.startsWith("SneakyDB ") == true -> cause.message!!
        else -> "unexpected ${cause.javaClass.simpleName}"
    }
}
