package com.sneakydb

import com.sneakydb.velocity.SilentServer
import org.bukkit.command.Command
import org.bukkit.command.CommandSender
import org.bukkit.configuration.file.YamlConfiguration
import org.bukkit.event.server.ServerLoadEvent
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockbukkit.mockbukkit.MockBukkit
import org.mockbukkit.mockbukkit.ServerMock
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/** MockBukkit's server with `shutdown()` recorded instead of unimplemented. */
class RecordingServer : ServerMock() {
    val shutdowns = AtomicInteger()
    override fun shutdown() { shutdowns.incrementAndGet() }
}

/**
 * The Paper startup hold: the startup migrations run while the server thread waits in `ServerLoadEvent` (STARTUP), and
 * what happens afterwards. MockBukkit's test thread is the server thread. Failing scripts need a database: see
 * [PostgreSqlStartupTest].
 */
class SneakyDBStartupTest {
    private lateinit var server: RecordingServer
    /** CMI commands, each with whether it ran on the server thread. */
    private val commands = CopyOnWriteArrayList<Pair<List<String>, Boolean>>()

    @BeforeEach fun start() {
        server = MockBukkit.mock(RecordingServer())
        MockBukkit.createMockPlugin("CMI")
        server.commandMap.register("cmi", object : Command("cmi") {
            override fun execute(sender: CommandSender, label: String, args: Array<out String>): Boolean {
                commands.add(args.toList() to server.isPrimaryThread)
                return true
            }
        })
    }

    @AfterEach fun stop() = MockBukkit.unmock()

    private fun load(vararg values: Pair<String, Any>): SneakyDB {
        val config = YamlConfiguration().apply {
            // An unsupported adapter fails initialization at once, without a network attempt.
            set("database.adapter", "unsupported")
            set("maintenance.message", "Database offline")
            values.forEach { (key, value) -> set(key, value) }
        }
        return MockBukkit.loadWithConfig(SneakyDB::class.java, config)
    }

    private fun startupEvent() = server.pluginManager.callEvent(ServerLoadEvent(ServerLoadEvent.LoadType.STARTUP))

    private fun ticks(count: Int) = repeat(count) { server.scheduler.performOneTick(); Thread.sleep(5) }

    private val maintenance = listOf("maintenance", "true", "Database", "offline")

    @Test fun `a plugin failing before its scripts ran holds startup until the run ends and switches maintenance on before it continues`() {
        load()
        val migration = SneakyDBProvider.getApi().migrate("plots", SneakyDBStartupTest::class.java)
        startupEvent()
        assertTrue(migration.isCompletedExceptionally, "the event returned only after the startup run ended")
        assertEquals(listOf(maintenance to true), commands, "sent on the server thread before startup continues")
        assertEquals(0, server.shutdowns.get(), "startup continues")
        ticks(20)
        assertEquals(listOf(maintenance to true), commands, "the outage path's scheduled command doesn't send a second one")
    }

    @Test fun `with nothing registered startup continues at once and an outage closes access at the first tick as before`() {
        load()
        startupEvent()
        assertTrue(commands.isEmpty(), "no startup run, nothing sent from the event")
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        while (commands.isEmpty() && System.nanoTime() < deadline) ticks(1)
        ticks(10)
        assertEquals(listOf(maintenance to true), commands)
        assertEquals(0, server.shutdowns.get())
    }

    @Test fun `a reload starts nothing, and an enable after startup runs its registrations at the first tick without holding`() {
        load()
        val migration = SneakyDBProvider.getApi().migrate("plots", SneakyDBStartupTest::class.java)
        server.pluginManager.callEvent(ServerLoadEvent(ServerLoadEvent.LoadType.RELOAD))
        Thread.sleep(100)
        assertFalse(migration.isDone, "RELOAD did not start a run")
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        while (!migration.isDone && System.nanoTime() < deadline) ticks(1)
        assertTrue(migration.isCompletedExceptionally, "the first tick ran it as a late batch")
        startupEvent()
        assertEquals(0, server.shutdowns.get())
    }

    @Test fun `disabling SneakyDB while startup waits releases the server thread`() {
        SilentServer().use { database ->
            val plugin = load("database.adapter" to "postgresql", "database.host" to "127.0.0.1", "database.port" to database.port,
                "database.connection-timeout-ms" to 30000)
            SneakyDBProvider.getApi().migrate("plots", SneakyDBStartupTest::class.java)
            val disabler = Thread {
                Thread.sleep(300)
                server.pluginManager.disablePlugin(plugin) // Paper's shutdown thread does this when the server thread doesn't stop
            }.apply { start() }
            val started = System.nanoTime()
            startupEvent()
            val waited = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)
            disabler.join()
            assertTrue(waited in 250..10_000, "waited $waited ms: until the disable, not until the 30 s connection timeout")
            assertTrue(commands.isEmpty(), commands.toString())
            assertEquals(0, server.shutdowns.get())
        }
    }
}
