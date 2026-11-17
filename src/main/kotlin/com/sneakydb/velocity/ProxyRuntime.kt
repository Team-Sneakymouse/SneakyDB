package com.sneakydb.velocity

import com.sneakydb.DatabaseSession
import com.sneakydb.LoginGate
import com.sneakydb.MigrationSettings
import com.sneakydb.SneakyDBRegistry
import com.sneakydb.logReadinessFailure
import com.sneakydb.startDatabaseSession
import java.nio.file.Path

/**
 * The proxy lifecycle, kept free of Velocity types so it can be tested without a proxy.
 * Same session and API as on Paper, without the CMI maintenance fallback and therefore without its health checks.
 */
internal class ProxyRuntime(
    private val directory: Path,
    private val info: (String) -> Unit,
    private val warn: (String) -> Unit,
    private val severe: (String) -> Unit,
) {
    private var session: DatabaseSession? = null
    @Volatile private var gate: LoginGate? = null

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
        // Closed from here until the startup migration run has finished without a failure.
        val gate = LoginGate(migrations.loginMessage)
        this.gate = gate
        val generation = startDatabaseSession(config, migrations, info, severe, warn)
        session = generation
        SneakyDBRegistry.api = generation
        generation.whenReady().whenComplete { _, failure ->
            if (failure != null) logReadinessFailure(severe, failure)
            else if (generation.isAvailable()) info("PostgreSQL adapter connected and ready.")
        }
        generation.migrations?.startup?.thenAccept { succeeded -> if (succeeded) gate.open() }
    }

    /** Every plugin's ProxyInitializeEvent handler has run: start the startup migration run. */
    fun initialized() {
        session?.migrations?.registrationClosed()
    }

    /** Null when players may log in, otherwise the message to refuse them with (also before [start]). */
    fun loginRefusal(): String? = gate?.refusal() ?: if (gate == null) MigrationSettings.DEFAULT_LOGIN_MESSAGE else null

    /** Same guarantees as Paper's onDisable: unregisters the provider and closes the session without waiting. */
    fun stop() {
        SneakyDBRegistry.api = null
        session?.close()
        session = null
    }
}
