package com.sneakydb

import io.papermc.paper.event.connection.PlayerConnectionValidateLoginEvent
import net.kyori.adventure.text.Component
import org.bukkit.event.EventHandler
import org.bukkit.event.EventPriority
import org.bukkit.event.Listener
import org.bukkit.event.player.AsyncPlayerPreLoginEvent
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
        // Closed from here until the startup migration run has finished; Bukkit drops the listener on disable.
        val gate = LoginGate(migrations.loginMessage)
        server.pluginManager.registerEvents(LoginGateListener(gate), this)
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
        // A failed startup run keeps the gate closed for this generation and closes access through CMI, once, as an outage does.
        coordinator.startup.thenAccept { succeeded -> if (succeeded) gate.open() else fallback.startupFailed() }
        // The first tick comes after every plugin has enabled, so every plugin has had its chance to register.
        server.scheduler.runTask(this, Runnable { coordinator.registrationClosed() })
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
 * Refuses logins while [gate] is closed, twice. Pre-login at the lowest priority refuses early, so later handlers can
 * see the login is already refused. A later handler can still allow it again, and a player whose pre-login ran before
 * this listener existed never met it, so the validate-login event refuses again at the monitor priority: Paper fires
 * it when the login starts and again when configuration finishes, the last point before the player joins the world.
 * It changes the result at the monitor priority on purpose, so no handler at a normal priority can override it.
 */
internal class LoginGateListener(private val gate: LoginGate) : Listener {
    @EventHandler(priority = EventPriority.LOWEST)
    fun onPreLogin(event: AsyncPlayerPreLoginEvent) {
        val refusal = gate.refusal() ?: return
        event.disallow(AsyncPlayerPreLoginEvent.Result.KICK_OTHER, Component.text(refusal))
    }

    @EventHandler(priority = EventPriority.MONITOR)
    fun onValidateLogin(event: PlayerConnectionValidateLoginEvent) {
        val refusal = gate.refusal() ?: return
        if (event.isAllowed) event.kickMessage(Component.text(refusal))
    }
}
