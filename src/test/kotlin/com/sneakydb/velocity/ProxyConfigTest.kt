package com.sneakydb.velocity

import com.sneakydb.DatabaseConfig
import org.bukkit.configuration.file.YamlConfiguration
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.spongepowered.configurate.yaml.YamlConfigurationLoader
import java.nio.file.Files
import java.nio.file.Path

class ProxyConfigTest {
    private val logged = mutableListOf<String>()

    private fun resource(name: String) = String(javaClass.getResourceAsStream("/$name")!!.readAllBytes())

    @Test fun `first start writes the default file, later starts keep the operator's edits`(@TempDir root: Path) {
        val directory = root.resolve("sneakydb")
        val defaults = ProxyConfig.load(directory, logged::add)
        val file = directory.resolve("config.yml")
        assertEquals(resource("velocity-config.yml"), Files.readString(file))
        assertEquals(1, logged.size)
        assertTrue(logged.single().contains(file.toString()))
        assertEquals("postgresql", defaults.adapterId)
        assertEquals("localhost", defaults.settings.host)
        assertEquals("change-me", defaults.settings.password)

        Files.writeString(file, Files.readString(file).replace("host: localhost", "host: db.internal"))
        logged.clear()
        assertEquals("db.internal", ProxyConfig.load(directory, logged::add).settings.host)
        assertTrue(logged.isEmpty(), "no second default write")
    }

    @Test fun `proxy default has exactly the Paper database keys and values`(@TempDir directory: Path) {
        val paper = YamlConfiguration().apply { loadFromString(resource("config.yml")) }
        val proxy = YamlConfigurationLoader.builder().buildAndLoadString(resource("velocity-config.yml")).node("database")
        assertEquals(paper.getConfigurationSection("database")!!.getKeys(false), proxy.childrenMap().keys.map { it.toString() }.toSet())
        val fromPaper = DatabaseConfig.read { paper.get("database.$it", null) }
        val fromProxy = ProxyConfig.load(directory) {}
        assertEquals(fromPaper.adapterId, fromProxy.adapterId)
        assertEquals(fromPaper.settings, fromProxy.settings)
    }

    @Test fun `values parse with Paper's defaults, coercion and bounds`(@TempDir directory: Path) {
        Files.writeString(directory.resolve("config.yml"), """
            database:
              adapter: PostgreSQL
              host: db.internal
              port: '6543'
              name: lom
              username: proxy
              password: 1234
              maximum-pool-size: 500
              connection-timeout-ms: 10
        """.trimIndent())
        val config = ProxyConfig.load(directory, logged::add)
        assertEquals("postgresql", config.adapterId)
        with(config.settings) {
            assertEquals("db.internal", host)
            assertEquals(5432, port, "a quoted number is a string, which Bukkit's getInt ignores too")
            assertEquals("lom", database)
            assertEquals("proxy", username)
            assertEquals("1234", password)
            assertEquals(50, maximumPoolSize)
            assertEquals(250, connectionTimeoutMs)
        }
        assertTrue(logged.isEmpty())
    }

    @Test fun `missing keys and section use defaults`(@TempDir directory: Path) {
        Files.writeString(directory.resolve("config.yml"), "other: true\n")
        val config = ProxyConfig.load(directory, logged::add)
        assertEquals("postgresql", config.adapterId)
        assertEquals(com.sneakydb.adapter.DatabaseSettings("localhost", 5432, "minecraft", "minecraft", "", 10, 5000), config.settings)
    }

    @Test fun `malformed yaml throws`(@TempDir directory: Path) {
        Files.writeString(directory.resolve("config.yml"), "database:\n  password: [secret\n")
        assertThrows(Exception::class.java) { ProxyConfig.load(directory, logged::add) }
    }
}
