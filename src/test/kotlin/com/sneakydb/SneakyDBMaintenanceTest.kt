package com.sneakydb

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.mockbukkit.mockbukkit.MockBukkit
import org.bukkit.command.Command
import org.bukkit.command.CommandSender
import org.bukkit.configuration.file.YamlConfiguration
import java.util.concurrent.TimeUnit

class SneakyDBMaintenanceTest {
    @Test fun `plugin startup failure reaches CMI console command on server thread`() {
        val server = MockBukkit.mock()
        try {
            MockBukkit.createMockPlugin("CMI")
            val commands = mutableListOf<List<String>>()
            server.commandMap.register("cmi", object : Command("cmi") {
                override fun execute(sender: CommandSender, label: String, args: Array<out String>): Boolean {
                    assertTrue(server.isPrimaryThread)
                    commands.add(args.toList())
                    return true
                }
            })
            val config = YamlConfiguration().apply {
                set("database.adapter", "unsupported")
                set("maintenance.message", "Database offline")
            }
            MockBukkit.loadWithConfig(SneakyDB::class.java, config)
            assertThrows(java.util.concurrent.ExecutionException::class.java) {
                SneakyDBProvider.getApi().whenReady().get(2, TimeUnit.SECONDS)
            }
            // whenComplete callbacks may still be enqueueing after the future settles.
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2)
            while (commands.isEmpty() && System.nanoTime() < deadline) {
                server.scheduler.performOneTick()
                Thread.sleep(5)
            }
            assertEquals(listOf(listOf("maintenance", "true", "Database", "offline")), commands)
        } finally { MockBukkit.unmock() }
    }
}
