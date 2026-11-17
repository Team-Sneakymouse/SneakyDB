package com.sneakydb.velocity

import com.google.inject.Inject
import com.velocitypowered.api.event.Subscribe
import com.velocitypowered.api.event.connection.PreLoginEvent
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent
import com.velocitypowered.api.event.proxy.ProxyShutdownEvent
import com.velocitypowered.api.plugin.annotation.DataDirectory
import net.kyori.adventure.text.Component
import org.slf4j.Logger
import java.nio.file.Path

/**
 * Velocity entry point (velocity-plugin.json, id `sneakydb`). References no Bukkit classes.
 *
 * Initialization runs at the highest priority, so the provider is registered before any other plugin's
 * ProxyInitializeEvent handler at normal order or later. A second handler at the lowest priority closes migration
 * registration after the other plugins' handlers. Shutdown runs at the lowest priority, after consumers' own shutdown
 * handlers, like Paper disabling SneakyDB after its dependents.
 */
class SneakyDBVelocity @Inject constructor(logger: Logger, @DataDirectory dataDirectory: Path) {
    private val runtime = ProxyRuntime(dataDirectory, logger::info, logger::warn, logger::error)

    @Subscribe(priority = Short.MAX_VALUE, async = false)
    fun onProxyInitialize(@Suppress("UNUSED_PARAMETER") event: ProxyInitializeEvent) = runtime.start()

    @Subscribe(priority = Short.MIN_VALUE, async = false)
    fun onProxyInitialized(@Suppress("UNUSED_PARAMETER") event: ProxyInitializeEvent) = runtime.initialized()

    /** Refuses logins until the startup migration run has finished without a failure; runs first, so others see it. */
    @Subscribe(priority = Short.MAX_VALUE)
    fun onPreLogin(event: PreLoginEvent) {
        val refusal = runtime.loginRefusal() ?: return
        event.result = PreLoginEvent.PreLoginComponentResult.denied(Component.text(refusal))
    }

    @Subscribe(priority = Short.MIN_VALUE, async = false)
    fun onProxyShutdown(@Suppress("UNUSED_PARAMETER") event: ProxyShutdownEvent) = runtime.stop()
}
