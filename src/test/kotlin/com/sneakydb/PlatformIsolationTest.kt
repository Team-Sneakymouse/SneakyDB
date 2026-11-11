package com.sneakydb

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit

/** One JAR serves both platforms: each entry point must link and run without the other platform's classes. */
class PlatformIsolationTest {
    private val velocityPath = listOf(
        "velocity.SneakyDBVelocity", "velocity.ProxyRuntime", "velocity.ProxyConfig", "DatabaseConfig",
        "DatabaseStartupKt", "DatabaseSession", "NotificationWorker", "SneakyDBProvider", "SneakyDBRegistry",
        "adapter.PostgreSqlAdapter", "adapter.DatabaseAdapterKt",
    )

    @Test fun `each entry point links without the other platform's API`() {
        isolated("org.bukkit.").use { loader -> velocityPath.forEach { link(loader, it) } }
        isolated("com.velocitypowered.", "org.spongepowered.").use { loader -> link(loader, "SneakyDB") }
    }

    @Test fun `proxy lifecycle runs without Bukkit and loads the bundled driver`(@TempDir directory: Path) {
        val closedPort = java.net.ServerSocket(0).use { it.localPort }
        Files.writeString(directory.resolve("config.yml"), """
            database:
              host: 127.0.0.1
              port: $closedPort
              password: hunter2
              connection-timeout-ms: 250
        """.trimIndent())
        val severe = java.util.concurrent.CopyOnWriteArrayList<String>()
        isolated("org.bukkit.").use { loader ->
            val runtimeType = Class.forName("com.sneakydb.velocity.ProxyRuntime", true, loader)
            val log: (String) -> Unit = {}
            val sink: (String) -> Unit = { severe.add(it) }
            val runtime = runtimeType.getConstructor(Path::class.java, Function1::class.java, Function1::class.java, Function1::class.java)
                .newInstance(directory, log, log, sink)
            runtimeType.getMethod("start").invoke(runtime)
            val api = Class.forName("com.sneakydb.SneakyDBProvider", true, loader).getMethod("getApi").invoke(null)
            val ready = Class.forName("com.sneakydb.SneakyDBApi", true, loader).getMethod("whenReady").invoke(api) as CompletableFuture<*>
            val failure = assertThrows(ExecutionException::class.java) { ready.get(10, TimeUnit.SECONDS) }
            // Acquisition, not adapter creation: Hikari found org.postgresql.Driver and tried the network.
            assertEquals(CONNECTION_ACQUISITION, failure.cause?.message)
            awaitLines(severe, 1)
            assertEquals(listOf(CONNECTION_ACQUISITION), severe)
            runtimeType.getMethod("stop").invoke(runtime)
        }
    }

    private fun link(loader: ClassLoader, type: String) {
        val loaded = Class.forName("com.sneakydb.$type", true, loader)
        assertDoesNotThrow(loaded::getDeclaredConstructors)
        assertDoesNotThrow(loaded::getDeclaredMethods)
        assertDoesNotThrow(loaded::getDeclaredFields)
    }

    /** Loads SneakyDB's own classes afresh; everything else from the test classpath except [forbidden] packages. */
    private fun isolated(vararg forbidden: String): URLClassLoader {
        val classes = DatabaseSession::class.java.protectionDomain.codeSource.location
        return object : URLClassLoader(arrayOf(classes), javaClass.classLoader) {
            override fun loadClass(name: String, resolve: Boolean): Class<*> {
                if (forbidden.any(name::startsWith)) throw ClassNotFoundException("Other platform is absent: $name")
                if (!name.startsWith("com.sneakydb.")) return super.loadClass(name, resolve)
                synchronized(getClassLoadingLock(name)) {
                    val loaded = findLoadedClass(name) ?: findClass(name)
                    if (resolve) resolveClass(loaded)
                    return loaded
                }
            }
        }
    }

    companion object {
        const val CONNECTION_ACQUISITION =
            "SneakyDB initialization failed during connection acquisition; check adapter configuration and database access"

        /** whenComplete logging may still run after a waiter on the future wakes up. */
        fun awaitLines(lines: List<String>, count: Int) {
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
            while (lines.size < count && System.nanoTime() < deadline) Thread.sleep(5)
            Thread.sleep(50) // and give an unexpected extra line a chance to show up
        }
    }
}
