package com.sneakydb

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
        val settings = database.settings
        val generation = startDatabaseSession(database, logger::warning)
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
