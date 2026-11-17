package com.sneakydb.velocity

import com.sneakydb.PlatformIsolationTest.Companion.CONNECTION_ACQUISITION
import com.sneakydb.PlatformIsolationTest.Companion.awaitLines
import com.sneakydb.SneakyDBProvider
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit

class ProxyRuntimeTest {
    private val info = CopyOnWriteArrayList<String>()
    private val warn = CopyOnWriteArrayList<String>()
    private val severe = CopyOnWriteArrayList<String>()

    private fun runtime(directory: Path) = ProxyRuntime(directory, info::add, warn::add, severe::add)

    @Test fun `unreachable database fails readiness with one safe severe line and stop unregisters`(@TempDir directory: Path) {
        val closedPort = java.net.ServerSocket(0).use { it.localPort }
        Files.writeString(directory.resolve("config.yml"), """
            database:
              host: 127.0.0.1
              port: $closedPort
              password: hunter2
              connection-timeout-ms: 250
        """.trimIndent())
        val runtime = runtime(directory)
        runtime.start()
        try {
            val api = SneakyDBProvider.getApi()
            assertFalse(api.isAvailable())
            val failure = assertThrows(ExecutionException::class.java) { api.whenReady().get(10, TimeUnit.SECONDS) }
            assertEquals(CONNECTION_ACQUISITION, failure.cause?.message)
            awaitLines(severe, 1)
            assertEquals(listOf(CONNECTION_ACQUISITION), severe)
            assertTrue(warn.isEmpty())
            assertTrue((info + warn + severe).none { "hunter2" in it })
        } finally { runtime.stop() }
        assertEquals("SneakyDB is not loaded", assertThrows(IllegalStateException::class.java) { SneakyDBProvider.getApi() }.message)
        assertEquals(1, severe.size, "shutdown adds no severe line")
    }

    @Test fun `unreadable config fails readiness during adapter creation without quoting the file`(@TempDir directory: Path) {
        Files.writeString(directory.resolve("config.yml"), "database:\n  password: [hunter2\n")
        val runtime = runtime(directory)
        runtime.start()
        try {
            val api = SneakyDBProvider.getApi()
            assertThrows(ExecutionException::class.java) { api.whenReady().get(5, TimeUnit.SECONDS) }
            awaitLines(severe, 1)
            assertEquals(listOf("SneakyDB initialization failed during adapter creation; check adapter configuration and database access"), severe)
            assertEquals(1, warn.size)
            assertTrue(warn.single().startsWith("Could not read ${directory.resolve("config.yml")} ("))
            assertTrue((info + warn + severe).none { "hunter2" in it })
            assertThrows(IllegalStateException::class.java) { api.listen("pushes", java.util.function.Consumer {}) }
        } finally { runtime.stop() }
    }

    @Test fun `stop before start and repeated stop are harmless`(@TempDir directory: Path) {
        val runtime = runtime(directory)
        runtime.stop()
        runtime.stop()
        assertTrue(info.isEmpty() && warn.isEmpty() && severe.isEmpty())
    }
}

class ProxyLoginGateTest {
    private val severe = CopyOnWriteArrayList<String>()
    private fun runtime(directory: Path) = ProxyRuntime(directory, {}, {}, severe::add)

    private fun unreachable(directory: Path, extra: String = "") {
        val closedPort = java.net.ServerSocket(0).use { it.localPort }
        Files.writeString(directory.resolve("config.yml"), "database:\n  host: 127.0.0.1\n  port: $closedPort\n  connection-timeout-ms: 250\n$extra")
    }

    private fun awaitOpen(runtime: ProxyRuntime) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        while (runtime.loginRefusal() != null && System.nanoTime() < deadline) Thread.sleep(5)
    }

    @Test fun `logins are refused until proxy initialisation has finished and nothing is pending`(@TempDir directory: Path) {
        unreachable(directory, "migrations:\n  login-message: Proxy starting\n")
        val runtime = runtime(directory)
        assertEquals(com.sneakydb.MigrationSettings.DEFAULT_LOGIN_MESSAGE, runtime.loginRefusal(), "refused before start")
        runtime.start()
        try {
            assertEquals("Proxy starting", runtime.loginRefusal())
            runtime.initialized()
            awaitOpen(runtime)
            assertNull(runtime.loginRefusal(), "no plugin registered: open as soon as that is known")
        } finally { runtime.stop() }
    }

    @Test fun `a failed startup run keeps the proxy closed`(@TempDir directory: Path) {
        unreachable(directory)
        val runtime = runtime(directory)
        runtime.start()
        try {
            val migration = SneakyDBProvider.getApi().migrate("proxyplugin", ProxyLoginGateTest::class.java)
            runtime.initialized()
            assertThrows(ExecutionException::class.java) { migration.get(10, TimeUnit.SECONDS) }
            Thread.sleep(50)
            assertEquals(com.sneakydb.MigrationSettings.DEFAULT_LOGIN_MESSAGE, runtime.loginRefusal())
            assertTrue(severe.any { it.startsWith("Migrations for proxyplugin failed: SneakyDB initialization failed during connection acquisition") }, severe.toString())
        } finally { runtime.stop() }
    }

    @Test fun `the PreLoginEvent handler denies with the refusal and leaves logins alone once open`(@TempDir directory: Path) {
        unreachable(directory, "migrations:\n  login-message: Proxy starting\n")
        val plugin = SneakyDBVelocity(org.slf4j.helpers.NOPLogger.NOP_LOGGER, directory)
        val connection = java.lang.reflect.Proxy.newProxyInstance(javaClass.classLoader,
            arrayOf(com.velocitypowered.api.proxy.InboundConnection::class.java)) { _, method, _ -> error(method.name) }
            as com.velocitypowered.api.proxy.InboundConnection
        fun login() = com.velocitypowered.api.event.connection.PreLoginEvent(connection, "player").also { plugin.onPreLogin(it) }.result
        plugin.onProxyInitialize(com.velocitypowered.api.event.proxy.ProxyInitializeEvent())
        try {
            val refused = login()
            assertFalse(refused.isAllowed)
            assertEquals(net.kyori.adventure.text.Component.text("Proxy starting"), refused.reasonComponent.get())
            plugin.onProxyInitialized(com.velocitypowered.api.event.proxy.ProxyInitializeEvent())
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
            while (!login().isAllowed && System.nanoTime() < deadline) Thread.sleep(5)
            assertTrue(login().isAllowed)
        } finally { plugin.onProxyShutdown(com.velocitypowered.api.event.proxy.ProxyShutdownEvent()) }
    }

    private fun <T> stub(type: Class<T>): T =
        type.cast(java.lang.reflect.Proxy.newProxyInstance(javaClass.classLoader, arrayOf(type)) { _, method, _ -> error(method.name) })

    /** Runs [plugin]'s @Subscribe handlers for [event] together with [others] in Velocity's order: higher priority first. */
    private fun dispatch(plugin: SneakyDBVelocity, event: Any, vararg others: Pair<Short, (Any) -> Unit>) {
        val own = plugin.javaClass.methods.filter { it.parameterTypes.singleOrNull() == event.javaClass }.mapNotNull { method ->
            method.getAnnotation(com.velocitypowered.api.event.Subscribe::class.java)?.let { subscribe ->
                subscribe.priority to { e: Any -> method.invoke(plugin, e); Unit }
            }
        }
        (own + others).sortedByDescending { it.first }.forEach { it.second(event) }
    }

    @Test fun `a later pre-login handler or a login that passed pre-login early can't get in while the gate is closed`(@TempDir directory: Path) {
        unreachable(directory, "migrations:\n  login-message: Proxy starting\n")
        val plugin = SneakyDBVelocity(org.slf4j.helpers.NOPLogger.NOP_LOGGER, directory)
        plugin.onProxyInitialize(com.velocitypowered.api.event.proxy.ProxyInitializeEvent())
        try {
            val preLogin = com.velocitypowered.api.event.connection.PreLoginEvent(stub(com.velocitypowered.api.proxy.InboundConnection::class.java), "player")
            val forceOnline: (Any) -> Unit = {
                (it as com.velocitypowered.api.event.connection.PreLoginEvent).result =
                    com.velocitypowered.api.event.connection.PreLoginEvent.PreLoginComponentResult.forceOnlineMode()
            }
            dispatch(plugin, preLogin, 0.toShort() to forceOnline)
            assertFalse(preLogin.result.isAllowed, "a consumer at normal order set forceOnlineMode() after SneakyDB's early refusal")
            assertEquals(net.kyori.adventure.text.Component.text("Proxy starting"), preLogin.result.reasonComponent.get())

            val player = stub(com.velocitypowered.api.proxy.Player::class.java)
            val login = com.velocitypowered.api.event.connection.LoginEvent(player)
            dispatch(plugin, login)
            assertFalse(login.result.isAllowed, "a player whose pre-login passed is refused at login while the gate is closed")

            plugin.onProxyInitialized(com.velocitypowered.api.event.proxy.ProxyInitializeEvent())
            awaitOpen(plugin)
            val later = com.velocitypowered.api.event.connection.LoginEvent(player)
            dispatch(plugin, later)
            assertTrue(later.result.isAllowed)
        } finally { plugin.onProxyShutdown(com.velocitypowered.api.event.proxy.ProxyShutdownEvent()) }
    }

    private fun awaitOpen(plugin: SneakyDBVelocity) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        val connection = stub(com.velocitypowered.api.proxy.InboundConnection::class.java)
        while (System.nanoTime() < deadline) {
            val event = com.velocitypowered.api.event.connection.PreLoginEvent(connection, "player")
            dispatch(plugin, event)
            if (event.result.isAllowed) return
            Thread.sleep(5)
        }
    }
}
