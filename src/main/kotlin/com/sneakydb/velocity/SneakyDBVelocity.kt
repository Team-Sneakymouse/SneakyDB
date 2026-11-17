package com.sneakydb.velocity

import com.google.inject.Inject
import com.velocitypowered.api.event.Subscribe
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent
import com.velocitypowered.api.event.proxy.ProxyShutdownEvent
import com.velocitypowered.api.plugin.annotation.DataDirectory
import com.velocitypowered.api.proxy.ProxyServer
import org.slf4j.Logger
import java.nio.file.Path

/**
 * Velocity entry point (velocity-plugin.json, id `sneakydb`). References no Bukkit classes.
 *
 * Initialization runs at the highest priority, so the provider is registered before any other plugin's
 * ProxyInitializeEvent handler at normal order or later. A second handler at the lowest priority runs the startup
 * migrations after the other plugins' handlers and blocks until they end: Velocity binds its listener only after the
 * event has completed. Shutdown runs at the lowest priority, after consumers' own shutdown handlers, like Paper
 * disabling SneakyDB after its dependents.
 */
class SneakyDBVelocity private constructor(private val runtime: ProxyRuntime) {
    @Inject constructor(proxy: ProxyServer, logger: Logger, @DataDirectory dataDirectory: Path) :
        this(ProxyRuntime(dataDirectory, logger::info, logger::warn, logger::error, { proxy.shutdown() }))

    /** For tests: [park] replaces waiting for the JVM to exit after asking the proxy to shut down. */
    internal constructor(proxy: ProxyServer, logger: Logger, dataDirectory: Path, park: () -> Unit) :
        this(ProxyRuntime(dataDirectory, logger::info, logger::warn, logger::error, { proxy.shutdown() }, park))

    @Subscribe(priority = Short.MAX_VALUE, async = false)
    fun onProxyInitialize(@Suppress("UNUSED_PARAMETER") event: ProxyInitializeEvent) = runtime.start()

    @Subscribe(priority = Short.MIN_VALUE, async = false)
    fun onProxyInitialized(@Suppress("UNUSED_PARAMETER") event: ProxyInitializeEvent) = runtime.initialized()

    @Subscribe(priority = Short.MIN_VALUE, async = false)
    fun onProxyShutdown(@Suppress("UNUSED_PARAMETER") event: ProxyShutdownEvent) = runtime.stop()
}
