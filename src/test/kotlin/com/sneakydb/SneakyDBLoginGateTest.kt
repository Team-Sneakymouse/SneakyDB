package com.sneakydb

import net.kyori.adventure.text.Component
import org.bukkit.command.Command
import org.bukkit.command.CommandSender
import org.bukkit.configuration.file.YamlConfiguration
import org.bukkit.event.player.AsyncPlayerPreLoginEvent
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockbukkit.mockbukkit.MockBukkit
import org.mockbukkit.mockbukkit.ServerMock
import java.util.UUID
import java.util.concurrent.CompletionException
import java.util.concurrent.TimeUnit

class SneakyDBLoginGateTest {
    private lateinit var server: ServerMock
    private val commands = mutableListOf<List<String>>()

    @BeforeEach fun start() {
        server = MockBukkit.mock()
        MockBukkit.createMockPlugin("CMI")
        server.commandMap.register("cmi", object : Command("cmi") {
            override fun execute(sender: CommandSender, label: String, args: Array<out String>): Boolean {
                commands.add(args.toList())
                return true
            }
        })
    }

    @AfterEach fun stop() = MockBukkit.unmock()

    /** Calls the pre-login event the way Paper does, off the server thread; returns the refusal message or null. */
    private fun preLogin(): String? {
        val event = AsyncPlayerPreLoginEvent("player", java.net.InetAddress.getLoopbackAddress(), UUID.randomUUID())
        val thread = Thread { server.pluginManager.callEvent(event) }
        thread.start()
        thread.join(5000)
        return if (event.loginResult == AsyncPlayerPreLoginEvent.Result.ALLOWED) null
        else net.kyori.adventure.text.serializer.plain.PlainTextComponentSerializer.plainText().serialize(event.kickMessage())
    }

    private fun load(vararg values: Pair<String, Any>) {
        val config = YamlConfiguration().apply {
            // An unsupported adapter fails initialization at once, without a network attempt.
            set("database.adapter", "unsupported")
            set("maintenance.message", "Database offline")
            values.forEach { (key, value) -> set(key, value) }
        }
        MockBukkit.loadWithConfig(SneakyDB::class.java, config)
    }

    private fun tickUntil(condition: () -> Boolean) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        while (!condition() && System.nanoTime() < deadline) {
            server.scheduler.performOneTick()
            Thread.sleep(5)
        }
    }

    @Test fun `logins are refused from enable until the first tick when no plugin registered`() {
        load("migrations.login-message" to "Starting up")
        assertEquals("Starting up", preLogin())
        tickUntil { preLogin() == null }
        assertNull(preLogin(), "nothing to migrate: the gate opens on the first tick")
    }

    @Test fun `a failed startup run keeps logins refused and closes access through CMI once`() {
        load()
        val migration = SneakyDBProvider.getApi().migrate("plots", SneakyDBLoginGateTest::class.java)
        assertEquals(MigrationSettings.DEFAULT_LOGIN_MESSAGE, preLogin())
        tickUntil { migration.isDone && commands.isNotEmpty() }
        val failure = assertThrows(CompletionException::class.java) { migration.join() }
        assertTrue(failure.cause!!.message!!.startsWith("Migrations for plots failed: SneakyDB initialization failed during adapter creation"),
            failure.cause!!.message)
        repeat(20) { server.scheduler.performOneTick(); Thread.sleep(5) }
        assertEquals(MigrationSettings.DEFAULT_LOGIN_MESSAGE, preLogin(), "the gate stays closed for this enable generation")
        assertEquals(listOf(listOf("maintenance", "true", "Database", "offline")), commands,
            "outage and migration failure share one CMI command")
    }

    @Test fun `the listener refuses with the configured message and lets logins through once open`() {
        val gate = LoginGate("Closed for now")
        val listener = LoginGateListener(gate)
        val event = AsyncPlayerPreLoginEvent("player", java.net.InetAddress.getLoopbackAddress(), UUID.randomUUID())
        listener.onPreLogin(event)
        assertEquals(AsyncPlayerPreLoginEvent.Result.KICK_OTHER, event.loginResult)
        assertEquals(Component.text("Closed for now"), event.kickMessage())
        gate.open()
        val later = AsyncPlayerPreLoginEvent("player", java.net.InetAddress.getLoopbackAddress(), UUID.randomUUID())
        listener.onPreLogin(later)
        assertEquals(AsyncPlayerPreLoginEvent.Result.ALLOWED, later.loginResult)
    }
}
