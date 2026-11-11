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
