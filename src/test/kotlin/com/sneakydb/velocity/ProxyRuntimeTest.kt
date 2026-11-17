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

/** A TCP server that accepts connections and never answers: a database that hangs during login. */
class SilentServer : AutoCloseable {
    private val socket = java.net.ServerSocket(0)
    private val accepted = CopyOnWriteArrayList<java.net.Socket>()
    val port get() = socket.localPort

    init {
        Thread {
            while (!socket.isClosed) runCatching { accepted.add(socket.accept()) }
        }.apply { isDaemon = true; start() }
    }

    override fun close() {
        socket.close()
        accepted.forEach { runCatching { it.close() } }
    }
}

class ProxyStartupTest {
    private val severe = CopyOnWriteArrayList<String>()
    private val shutdowns = java.util.concurrent.atomic.AtomicInteger()
    private val parks = java.util.concurrent.atomic.AtomicInteger()

    private fun runtime(directory: Path) =
        ProxyRuntime(directory, {}, {}, severe::add, { shutdowns.incrementAndGet() }, { parks.incrementAndGet() })

    private fun unreachable(directory: Path) {
        val closedPort = java.net.ServerSocket(0).use { it.localPort }
        Files.writeString(directory.resolve("config.yml"), "database:\n  host: 127.0.0.1\n  port: $closedPort\n  connection-timeout-ms: 250\n")
    }

    /** Runs [block] on another thread, as Velocity's initialisation would; the future completes when it returns. */
    private fun async(block: () -> Unit) = java.util.concurrent.CompletableFuture.runAsync(block)

    @Test fun `with nothing registered initialisation returns at once and the proxy keeps running`(@TempDir directory: Path) {
        unreachable(directory)
        val runtime = runtime(directory)
        runtime.start()
        try {
            runtime.initialized()
            assertEquals(0, shutdowns.get())
            assertEquals(0, parks.get())
        } finally { runtime.stop() }
    }

    @Test fun `a plugin failing before its scripts ran holds initialisation until the run ends, then shuts the proxy down`(@TempDir directory: Path) {
        unreachable(directory)
        val runtime = runtime(directory)
        runtime.start()
        try {
            val migration = SneakyDBProvider.getApi().migrate("proxyplugin", ProxyStartupTest::class.java)
            val initialization = async { runtime.initialized() }
            initialization.get(10, TimeUnit.SECONDS)
            assertTrue(migration.isCompletedExceptionally, "initialisation returned only after the run ended")
            assertEquals(1, shutdowns.get())
            assertEquals(1, parks.get(), "then waits for the JVM to exit instead of letting the proxy bind")
            assertTrue(severe.any { it.startsWith("Migrations for proxyplugin failed: SneakyDB initialization failed during connection acquisition") }, severe.toString())
            assertTrue(severe.any { it.contains("A proxy has no maintenance fallback, so SneakyDB shuts it down") }, severe.toString())
        } finally { runtime.stop() }
    }

    @Test fun `stopping SneakyDB while initialisation waits releases it`(@TempDir directory: Path) {
        SilentServer().use { database ->
            Files.writeString(directory.resolve("config.yml"),
                "database:\n  host: 127.0.0.1\n  port: ${database.port}\n  connection-timeout-ms: 30000\n")
            val runtime = runtime(directory)
            runtime.start()
            SneakyDBProvider.getApi().migrate("proxyplugin", ProxyStartupTest::class.java)
            val initialization = async { runtime.initialized() }
            Thread.sleep(300)
            assertFalse(initialization.isDone, "the database hangs: the run is still waiting for readiness")
            runtime.stop() // what ProxyShutdownEvent does
            initialization.get(5, TimeUnit.SECONDS)
            assertEquals(0, shutdowns.get(), "the proxy is already shutting down")
            assertEquals(1, parks.get())
        }
    }

    private fun <T> stub(type: Class<T>, calls: (String) -> Unit = {}): T =
        type.cast(java.lang.reflect.Proxy.newProxyInstance(javaClass.classLoader, arrayOf(type)) { _, method, _ -> calls(method.name); null })

    @Test fun `the Velocity entry point shuts the proxy down through ProxyServer after a failed startup run`(@TempDir directory: Path) {
        unreachable(directory)
        val calls = CopyOnWriteArrayList<String>()
        val proxy = stub(com.velocitypowered.api.proxy.ProxyServer::class.java, calls::add)
        val plugin = SneakyDBVelocity(proxy, org.slf4j.helpers.NOPLogger.NOP_LOGGER, directory) { parks.incrementAndGet() }
        plugin.onProxyInitialize(com.velocitypowered.api.event.proxy.ProxyInitializeEvent())
        try {
            SneakyDBProvider.getApi().migrate("proxyplugin", ProxyStartupTest::class.java)
            plugin.onProxyInitialized(com.velocitypowered.api.event.proxy.ProxyInitializeEvent())
            assertEquals(listOf("shutdown"), calls)
            assertEquals(1, parks.get())
        } finally { plugin.onProxyShutdown(com.velocitypowered.api.event.proxy.ProxyShutdownEvent()) }
    }

    @Test fun `the Velocity entry point lets initialisation finish when nothing failed`(@TempDir directory: Path) {
        unreachable(directory)
        val calls = CopyOnWriteArrayList<String>()
        val plugin = SneakyDBVelocity(stub(com.velocitypowered.api.proxy.ProxyServer::class.java, calls::add),
            org.slf4j.helpers.NOPLogger.NOP_LOGGER, directory) { parks.incrementAndGet() }
        plugin.onProxyInitialize(com.velocitypowered.api.event.proxy.ProxyInitializeEvent())
        try {
            plugin.onProxyInitialized(com.velocitypowered.api.event.proxy.ProxyInitializeEvent())
            assertTrue(calls.isEmpty(), calls.toString())
            assertEquals(0, parks.get())
        } finally { plugin.onProxyShutdown(com.velocitypowered.api.event.proxy.ProxyShutdownEvent()) }
    }
}
