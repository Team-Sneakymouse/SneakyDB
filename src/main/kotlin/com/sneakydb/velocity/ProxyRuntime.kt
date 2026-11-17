package com.sneakydb.velocity

import com.sneakydb.DatabaseSession
import com.sneakydb.MigrationSettings
import com.sneakydb.SneakyDBRegistry
import com.sneakydb.StartupOutcome
import com.sneakydb.awaitStartup
import com.sneakydb.logReadinessFailure
import com.sneakydb.startDatabaseSession
import java.nio.file.Path

/**
 * The proxy lifecycle, kept free of Velocity types so it can be tested without a proxy.
 * Same session and API as on Paper, without the CMI maintenance fallback and therefore without its health checks.
 * [shutdownProxy] asks the proxy to shut down (it does so on another thread and exits the JVM); [park] then keeps the
 * initialisation thread from going on to accept connections until that happens.
 */
internal class ProxyRuntime @JvmOverloads constructor(
    private val directory: Path,
    private val info: (String) -> Unit,
    private val warn: (String) -> Unit,
    private val severe: (String) -> Unit,
    private val shutdownProxy: () -> Unit = {},
    private val park: () -> Unit = ::parkForever,
) {
    private var session: DatabaseSession? = null

    /** Registers the provider with readiness pending; never throws for bad config or an unreachable database. */
    fun start() {
        var migrations = MigrationSettings.DEFAULTS
        val config = try {
            ProxyConfig.load(directory, info).also { migrations = ProxyConfig.migrations(directory, warn) }
        } catch (error: Exception) {
            // Parser messages can quote file content (passwords), so name the error class only.
            warn("Could not read ${directory.resolve(ProxyConfig.FILE)} (${error.javaClass.simpleName}); not connecting.")
            null
        }
        val generation = startDatabaseSession(config, migrations, info, severe, warn)
        session = generation
        SneakyDBRegistry.api = generation
        generation.whenReady().whenComplete { _, failure ->
            if (failure != null) logReadinessFailure(severe, failure)
            else if (generation.isAvailable()) info("PostgreSQL adapter connected and ready.")
        }
    }

    /**
     * Every plugin's ProxyInitializeEvent handler has run: runs the startup migrations and blocks until they end, so the
     * proxy binds its listener only afterwards. Any failure shuts the proxy down: it has no maintenance fallback.
     */
    fun initialized() {
        val coordinator = session?.migrations ?: return
        coordinator.registrationClosed(startupRun = true)
        when (awaitStartup(coordinator.startup)) {
            StartupOutcome.CLEAN -> return
            StartupOutcome.PLUGINS_FAILED -> {
                severe("Migrations failed for some plugins before any of their scripts ran (see the lines above). A proxy has no " +
                    "maintenance fallback, so SneakyDB shuts it down; fix the cause and start it again.")
                shutdownProxy()
            }
            StartupOutcome.STOPPED -> shutdownProxy()
            // SneakyDB was stopped: the proxy is shutting down.
            null -> {}
        }
        park()
    }

    /** Same guarantees as Paper's onDisable: unregisters the provider and closes the session without waiting. */
    fun stop() {
        SneakyDBRegistry.api = null
        session?.close()
        session = null
    }
}

/** Blocks the calling thread until the JVM exits. */
private fun parkForever() {
    val never = java.util.concurrent.CountDownLatch(1)
    while (true) {
        try { never.await() } catch (_: InterruptedException) {}
    }
}
