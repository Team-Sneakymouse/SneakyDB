package com.sneakydb

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

// Plugin-owned migrations: registration, the startup run, late batches and the login gate. No platform types here.

/** The `migrations:` config section, read with the same rules on Paper and Velocity. */
internal class MigrationSettings(val mode: MigrationMode, val lockTimeoutSeconds: Int, val loginMessage: String) {
    companion object {
        const val DEFAULT_LOGIN_MESSAGE = "The server is starting. Please try again in a moment."
        val DEFAULTS = MigrationSettings(MigrationMode.APPLY, 10, DEFAULT_LOGIN_MESSAGE)

        /**
         * [value] returns the raw value under `migrations.<key>`, or null when absent. A missing mode means apply; an
         * unknown one means verify, with a warning, so a typo never lets a server write to the database.
         */
        fun read(value: (String) -> Any?, warn: (String) -> Unit): MigrationSettings {
            val rawMode = value("mode")?.toString()?.trim()?.lowercase()
            val mode = when (rawMode) {
                null, "apply" -> MigrationMode.APPLY
                "verify" -> MigrationMode.VERIFY
                else -> MigrationMode.VERIFY.also { warn("migrations.mode must be apply or verify; running in verify mode") }
            }
            return MigrationSettings(
                mode,
                ((value("lock-timeout-seconds") as? Number)?.toInt() ?: 10).coerceIn(1, 300),
                playerMessage(value("login-message")?.toString(), DEFAULT_LOGIN_MESSAGE),
            )
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
     * applied ([recheckStep]). Returns false when another server applied it in the meantime.
     */
    fun apply(step: PlannedStep, lockTimeoutSeconds: Int): Boolean

    /** Under the migration lock, writing nothing: throws unless [plugin]'s rows are exactly [scripts] ([recheckReady]). */
    fun confirmReady(plugin: String, scripts: List<MigrationScript>)
}

/** Refuses logins from SneakyDB's enable until the startup migration run has finished without failures. */
internal class LoginGate(val message: String) {
    @Volatile private var closed = true
    fun open() { closed = false }
    /** Null when logins are allowed, otherwise the message to refuse them with. */
    fun refusal(): String? = if (closed) message else null
}

internal fun migrationThread(): ExecutorService = ThreadPoolExecutor(1, 1, 30, TimeUnit.SECONDS, LinkedBlockingQueue()) { task ->
    Thread(task, "SneakyDB-Migrations").apply {
        isDaemon = true
        contextClassLoader = DatabaseSession::class.java.classLoader
    }
}.apply { allowCoreThreadTimeOut(true) }

/**
 * One enable generation's migrations. [register] only records a plugin; [registrationClosed] (first tick on Paper,
 * after proxy initialisation on Velocity) starts the startup run, and later registrations run as their own batches.
 * Batches run one after another on [executor]. [startup] completes with true once the startup run finished without a
 * failed plugin, false otherwise; it never completes normally after [close].
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
    private val pending = HashMap<String, Registration>()
    /** Every plugin that registered in this enable generation, with the reason its latest registration failed, if it did. */
    private val generation = HashMap<String, String?>()
    val startup = CompletableFuture<Boolean>()

    fun register(plugin: String, anchor: Class<*>): CompletableFuture<Int> = register(plugin) { discoverMigrationScripts(plugin, anchor) }

    internal fun register(plugin: String, scripts: () -> List<MigrationScript>): CompletableFuture<Int> {
        val result = CompletableFuture<Int>()
        if (!PLUGIN_ID.matches(plugin)) {
            result.completeExceptionally(IllegalArgumentException(
                "Migration plugin id '$plugin' must match [a-z][a-z0-9_]{0,62}; pass the plugin's lower-case IDENTIFIER"))
            return result
        }
        val registration = Registration(plugin, scripts, result)
        val late = synchronized(lock) {
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
            if (registering) startupBatch.add(registration)
            !registering
        }
        result.whenComplete { _, failure ->
            synchronized(lock) {
                pending.remove(plugin, registration)
                generation[plugin] = failure?.let { describeFailure(it) }
            }
        }
        if (late) submit(listOf(registration), "late registration of $plugin", startupRun = false)
        return result
    }

    /** Every plugin has had its chance to register: start the startup run. Later calls do nothing. */
    fun registrationClosed() {
        val batch = synchronized(lock) {
            if (!registering || stopped) return
            registering = false
            startupBatch.toList().also { startupBatch.clear() }
        }
        if (batch.isEmpty()) startup.complete(true)
        else submit(batch, "startup", startupRun = true)
    }

    fun close() {
        val outstanding = synchronized(lock) {
            stopped = true
            pending.values.toList().also { startupBatch.clear() }
        }
        val error = IllegalStateException("SneakyDB has shut down")
        outstanding.forEach { it.result.completeExceptionally(error) }
        startup.completeExceptionally(error)
        executor.shutdownNow()
    }

    private val isStopped get() = synchronized(lock) { stopped }

    private fun submit(batch: List<Registration>, label: String, startupRun: Boolean) {
        try {
            executor.execute {
                var succeeded = false
                try {
                    succeeded = runBatch(batch, label)
                } catch (error: Throwable) {
                    if (!isStopped) severe("Migrations ($label) stopped: ${describeFailure(error)}")
                } finally {
                    // Never leave a future pending, even if the run itself broke.
                    val error = MigrationException("Migrations stopped before they finished; see the server log")
                    batch.forEach { if (!it.result.isDone) { succeeded = false; it.result.completeExceptionally(error) } }
                    if (startupRun) startup.complete(succeeded)
                }
            }
        } catch (_: RejectedExecutionException) {
            val error = IllegalStateException("SneakyDB has shut down")
            batch.forEach { it.result.completeExceptionally(error) }
            startup.completeExceptionally(error)
        }
    }

    /** Runs one batch to the end; returns true when no plugin of the batch failed. */
    private fun runBatch(batch: List<Registration>, label: String): Boolean {
        val started = System.nanoTime()
        val run = BatchRun(batch.associateBy { it.plugin })
        try {
            ready().join()
        } catch (error: Throwable) {
            batch.forEach { run.fail(it.plugin, null, describeFailure(error)) }
            return run.finish(label, started)
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
            return run.finish(label, started)
        }
        // Plugins of earlier batches of this generation count as present, failed ones included.
        val earlier = synchronized(lock) { generation.filterKeys { it !in discovered } }
        val plan = planMigrations(discovered, applied, settings.mode, earlier)
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
                run.fail(step.plugin, step.script.fileName, describeFailure(error))
                run.skipFrom(step.key, plan, "${step.plugin}:${step.script.fileName}")
                continue
            }
            val left = remaining.merge(step.plugin, -1, Int::plus)!!
            if (left == 0) run.ready(step.plugin, plan)
        }
        return run.finish(label, started)
    }

    /** Outcome bookkeeping of one batch. */
    private inner class BatchRun(private val registrations: Map<String, Registration>) {
        val failed = LinkedHashSet<String>()
        val skipped = HashSet<StepKey>()
        private val completed = HashSet<String>()
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

        fun fail(plugin: String, script: String?, reason: String) {
            if (!failed.add(plugin)) return
            val registration = registrations[plugin] ?: return
            if (isStopped) {
                registration.result.completeExceptionally(IllegalStateException("SneakyDB has shut down"))
                return
            }
            val message = "Migrations for $plugin failed${script?.let { " at $it" } ?: ""}: $reason"
            severe(message)
            registration.result.completeExceptionally(MigrationException(message))
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

        fun finish(label: String, started: Long): Boolean {
            val millis = (System.nanoTime() - started) / 1_000_000
            val others = if (skippedByOthers > 0) " ($skippedByOthers already applied by another server)" else ""
            val outcome = if (failed.isEmpty()) "" else "; failed: ${failed.joinToString()}"
            if (!isStopped) {
                info("Migrations ($label, ${settings.mode.name.lowercase()} mode): $applied scripts applied$others, " +
                    "${completed.size} of ${registrations.size} plugins ready$outcome in $millis ms.")
            }
            return failed.isEmpty() && !isStopped
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
