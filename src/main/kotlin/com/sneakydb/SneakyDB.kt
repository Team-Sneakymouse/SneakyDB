package com.sneakydb

import org.bukkit.event.EventHandler
import org.bukkit.event.EventPriority
import org.bukkit.event.Listener
import org.bukkit.event.server.ServerLoadEvent
import org.bukkit.plugin.java.JavaPlugin

/** Paper entry point (paper-plugin.yml). The Velocity entry point is com.sneakydb.velocity.SneakyDBVelocity. */
open class SneakyDB : JavaPlugin() {
    private var session: DatabaseSession? = null
    private var health: DatabaseHealth? = null
    private var healthWorker: HealthWorker? = null

    override fun onEnable() {
        saveDefaultConfig()
        // An explicit null default keeps Bukkit's getX(path, default) semantics: the jar's defaults don't apply.
        val database = DatabaseConfig.read { config.get("database.$it", null) }
        val migrations = MigrationSettings.read({ config.get("migrations.$it", null) }, logger::warning)
        val settings = database.settings
        val generation = startDatabaseSession(database, migrations, logger::info, logger::severe, logger::warning)
        session = generation
        SneakyDBRegistry.api = generation
        val cmi = CmiMaintenance(
            { server.pluginManager.isPluginEnabled("CMI") },
            { server.dispatchCommand(server.consoleSender, it) },
            { logger.severe(it) },
        )
        val fallback = OutageFallback(
            config.getString("maintenance.message", "Database unavailable. Please try again later.")!!,
            { action -> server.scheduler.runTask(this, Runnable { action() }) },
            cmi::activate,
        )
        val worker = HealthWorker(
            { createHealthSource(settings) },
            config.getLong("maintenance.health-check-interval-seconds", 30).coerceIn(5, 300) * 1000,
        )
        healthWorker = worker
        health = DatabaseHealth(generation.whenReady(), fallback, worker::probe, worker::schedule)
        generation.whenReady().whenComplete { _, failure ->
            if (failure != null) logReadinessFailure(logger, failure)
            else if (generation.isAvailable()) logger.info("PostgreSQL adapter connected and ready.")
        }
        val coordinator = checkNotNull(generation.migrations)
        // Bukkit drops the listener on disable.
        server.pluginManager.registerEvents(StartupListener { holdStartup(coordinator, fallback) }, this)
        // Enabled after the server started (re-enable, /reload): no ServerLoadEvent at startup comes, so what registers
        // during this enable runs at the first tick as a late batch, without holding the server thread.
        server.scheduler.runTask(this, Runnable { coordinator.registrationClosed(startupRun = false) })
    }

    /**
     * On the server thread, after every plugin has enabled and before the server accepts connections: runs the startup
     * migrations while the thread waits. Ends early when SneakyDB is disabled (on shutdown, Paper disables plugins from
     * its shutdown thread when the server thread doesn't stop within a second).
     */
    private fun holdStartup(coordinator: MigrationCoordinator, fallback: OutageFallback) {
        coordinator.registrationClosed(startupRun = true)
        when (awaitStartup(coordinator.startup)) {
            StartupOutcome.PLUGINS_FAILED -> {
                logger.severe("Migrations failed for some plugins before any of their scripts ran (see the lines above); those " +
                    "plugins stay inactive. Startup continues with CMI maintenance on: once the cause is fixed and the server " +
                    "restarted, switch it off by hand (cmi maintenance false).")
                fallback.activateNow()
            }
            StartupOutcome.STOPPED -> server.shutdown()
            StartupOutcome.CLEAN, null -> {}
        }
    }

    override fun onDisable() {
        SneakyDBRegistry.api = null
        health?.close()
        health = null
        healthWorker?.close()
        healthWorker = null
        session?.close()
        session = null
    }
}

/**
 * Runs [hold] at `ServerLoadEvent` with load type `STARTUP`. Paper fires it on the server thread once every plugin has
 * enabled, right before it lets the server's network channels accept connections and before the first tick; a reload
 * fires it with `RELOAD`, which starts nothing. Monitor priority, so registrations from other handlers of the same event
 * are in the startup run.
 */
internal class StartupListener(private val hold: () -> Unit) : Listener {
    @EventHandler(priority = EventPriority.MONITOR)
    fun onServerLoad(event: ServerLoadEvent) {
        if (event.type == ServerLoadEvent.LoadType.STARTUP) hold()
    }
}
