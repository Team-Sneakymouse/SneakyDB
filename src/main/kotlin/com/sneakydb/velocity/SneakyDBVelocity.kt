package com.sneakydb.velocity

import com.google.inject.Inject
import com.velocitypowered.api.event.Subscribe
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent
import com.velocitypowered.api.event.proxy.ProxyShutdownEvent
import com.velocitypowered.api.plugin.annotation.DataDirectory
import org.slf4j.Logger
import java.nio.file.Path

/**
 * Velocity entry point (velocity-plugin.json, id `sneakydb`). References no Bukkit classes.
 *
 * Initialization runs at the highest priority, so the provider is registered before any other plugin's
 * ProxyInitializeEvent handler at normal order or later. Shutdown runs at the lowest priority, after consumers'
 * own shutdown handlers, like Paper disabling SneakyDB after its dependents.
 */
class SneakyDBVelocity @Inject constructor(logger: Logger, @DataDirectory dataDirectory: Path) {
    private val runtime = ProxyRuntime(dataDirectory, logger::info, logger::warn, logger::error)

    @Subscribe(priority = Short.MAX_VALUE, async = false)
    fun onProxyInitialize(@Suppress("UNUSED_PARAMETER") event: ProxyInitializeEvent) = runtime.start()

    @Subscribe(priority = Short.MIN_VALUE, async = false)
    fun onProxyShutdown(@Suppress("UNUSED_PARAMETER") event: ProxyShutdownEvent) = runtime.stop()
}
