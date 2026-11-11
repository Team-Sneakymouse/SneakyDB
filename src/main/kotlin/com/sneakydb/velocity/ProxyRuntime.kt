package com.sneakydb.velocity

import com.sneakydb.DatabaseSession
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

    /** Registers the provider with readiness pending; never throws for bad config or an unreachable database. */
    fun start() {
        val config = try {
            ProxyConfig.load(directory, info)
        } catch (error: Exception) {
            // Parser messages can quote file content (passwords), so name the error class only.
            warn("Could not read ${directory.resolve(ProxyConfig.FILE)} (${error.javaClass.simpleName}); not connecting.")
            null
        }
        val generation = startDatabaseSession(config, warn)
        session = generation
        SneakyDBRegistry.api = generation
        generation.whenReady().whenComplete { _, failure ->
            if (failure != null) logReadinessFailure(severe, failure)
            else if (generation.isAvailable()) info("PostgreSQL adapter connected and ready.")
        }
    }

    /** Same guarantees as Paper's onDisable: unregisters the provider and closes the session without waiting. */
    fun stop() {
        SneakyDBRegistry.api = null
        session?.close()
        session = null
    }
}
