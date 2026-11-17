package com.sneakydb

import com.sneakydb.MigrationScriptsTest.Companion.scripts
import com.sneakydb.adapter.DatabaseSettings
import com.sneakydb.velocity.ProxyRuntime
import org.bukkit.command.Command
import org.bukkit.command.CommandSender
import org.bukkit.configuration.file.YamlConfiguration
import org.bukkit.event.server.ServerLoadEvent
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.mockbukkit.mockbukkit.MockBukkit
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * The startup hold on Paper (MockBukkit) and Velocity ([ProxyRuntime]) against a disposable PostgreSQL named by
 * SNEAKYDB_TEST_PG_HOST, like [PostgreSqlMigrationTest]: each test gets a new database with the `sneakydb` schema.
 */
class PostgreSqlStartupTest {
    private val env = System.getenv()
    private val admin = env["SNEAKYDB_TEST_PG_HOST"]?.let { host ->
        DatabaseSettings(
            host, env["SNEAKYDB_TEST_PG_PORT"]?.toInt() ?: 5432, env["SNEAKYDB_TEST_PG_DB"] ?: "postgres",
            env["SNEAKYDB_TEST_PG_USER"] ?: "postgres", env["SNEAKYDB_TEST_PG_PASSWORD"] ?: "test", 2, 5000,
        )
    }
    private val database = "sneakydb_startup_test_${System.nanoTime()}"
    private var server: RecordingServer? = null
    private val commands = CopyOnWriteArrayList<List<String>>()

    private fun connect(name: String = database) =
        java.sql.DriverManager.getConnection("jdbc:postgresql://${admin!!.host}:${admin.port}/$name", admin.username, admin.password)

    private fun sql(statement: String) = connect().use { c -> c.createStatement().use { it.execute(statement) } }

    private fun exists(relation: String) =
        connect().use { c -> c.createStatement().use { s -> s.executeQuery("select to_regclass('$relation') is not null").use { it.next(); it.getBoolean(1) } } }

    @BeforeEach fun createDatabase() {
        assumeTrue(admin != null, "SNEAKYDB_TEST_PG_HOST not set")
        connect(admin!!.database).use { c -> c.createStatement().use { it.execute("create database $database") } }
        sql("create schema sneakydb")
    }

    @AfterEach fun dropDatabase() {
        if (admin == null) return
        if (server != null) MockBukkit.unmock()
        Thread.sleep(200) // pool closure runs on its own thread
        connect(admin.database).use { c -> c.createStatement().use { it.execute("drop database if exists $database with (force)") } }
    }

    /** Registers [files] as [plugin]'s scripts with the session SneakyDB published. */
    private fun register(plugin: String, vararg files: Pair<String, String>): CompletableFuture<Int> =
        (SneakyDBProvider.getApi() as DatabaseSession).migrations!!.register(plugin) { scripts(plugin, *files) }

    private val failing = "V001__broken.sql" to "create table broken_early (id int);\ninsert into missing_table values (1);"
    private val unmet = "V001__needs.sql" to "-- requires: absent >= 1\ncreate table needs_table (id int);"
    private val other = "V001__other.sql" to "create table other_table (id int);"

    // Paper

    private fun paper(lockTimeout: Int = 10) {
        val mock = MockBukkit.mock(RecordingServer())
        server = mock
        MockBukkit.createMockPlugin("CMI")
        mock.commandMap.register("cmi", object : Command("cmi") {
            override fun execute(sender: CommandSender, label: String, args: Array<out String>): Boolean {
                assertTrue(mock.isPrimaryThread)
                commands.add(args.toList())
                return true
            }
        })
        val config = YamlConfiguration().apply {
            set("database.host", admin!!.host)
            set("database.port", admin.port)
            set("database.name", database)
            set("database.username", admin.username)
            set("database.password", admin.password)
            set("maintenance.message", "Database offline")
            set("migrations.lock-timeout-seconds", lockTimeout)
        }
        MockBukkit.loadWithConfig(SneakyDB::class.java, config)
    }

    private fun startupEvent() = server!!.pluginManager.callEvent(ServerLoadEvent(ServerLoadEvent.LoadType.STARTUP))

    private fun ticks(count: Int) = repeat(count) { server!!.scheduler.performOneTick(); Thread.sleep(5) }

    @Test fun `Paper's server thread waits for the startup run, including a script waiting for a lock`() {
        sql("create table busy (id int)")
        paper(lockTimeout = 30)
        val plots = register("plots", "V001__alter.sql" to "alter table busy add column name text;")
        connect().use { holder ->
            holder.autoCommit = false
            holder.createStatement().use { it.execute("lock table busy in access exclusive mode") }
            val releaser = Thread { Thread.sleep(1500); holder.rollback() }.apply { start() }
            val started = System.nanoTime()
            startupEvent()
            val waited = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)
            releaser.join()
            assertTrue(waited >= 1400, "the event returned after $waited ms, before the lock was released")
        }
        assertEquals(1, plots.getNow(null), "the script ran once the lock was free, before startup continued")
        assertEquals(0, server!!.shutdowns.get())
        ticks(10)
        assertTrue(commands.isEmpty(), commands.toString())
    }

    @Test fun `on Paper a script failing while running stops the server and switches nothing on`() {
        paper()
        val broken = register("broken", failing)
        val zother = register("zother", other)
        startupEvent()
        assertEquals(1, server!!.shutdowns.get())
        assertTrue(commands.isEmpty(), "no CMI maintenance: nothing is left to switch off after the fix")
        assertTrue(broken.isCompletedExceptionally)
        assertTrue(zother.isCompletedExceptionally, "no further script ran")
        assertFalse(exists("broken_early"))
        assertFalse(exists("other_table"))
        ticks(10)
        assertTrue(commands.isEmpty(), commands.toString())
    }

    @Test fun `on Paper a plugin failing before its scripts ran lets startup continue with maintenance once and leaves others migrated`() {
        paper()
        val needs = register("needs", unmet)
        val zother = register("zother", other)
        startupEvent()
        assertEquals(listOf(listOf("maintenance", "true", "Database", "offline")), commands, "sent before startup continued")
        assertEquals(0, server!!.shutdowns.get())
        assertTrue(needs.isCompletedExceptionally)
        assertEquals(1, zother.getNow(null))
        assertTrue(exists("other_table"))
        assertFalse(exists("needs_table"))
        ticks(10)
        assertEquals(1, commands.size)
    }

    @Test fun `on Paper both kinds of failure in one run stop the server`() {
        paper()
        val needs = register("needs", unmet)
        val broken = register("broken", failing)
        startupEvent()
        assertEquals(1, server!!.shutdowns.get())
        assertTrue(commands.isEmpty(), commands.toString())
        assertTrue(needs.isCompletedExceptionally && broken.isCompletedExceptionally)
    }

    // Velocity

    private val proxyShutdowns = AtomicInteger()

    private fun proxy(directory: Path): ProxyRuntime {
        Files.writeString(directory.resolve("config.yml"), """
            database:
              host: ${admin!!.host}
              port: ${admin.port}
              name: $database
              username: ${admin.username}
              password: ${admin.password}
        """.trimIndent())
        return ProxyRuntime(directory, {}, {}, {}, { proxyShutdowns.incrementAndGet() }, {}).also { it.start() }
    }

    @Test fun `on Velocity a script failing while running shuts the proxy down`(@TempDir directory: Path) {
        val runtime = proxy(directory)
        try {
            val broken = register("broken", failing)
            runtime.initialized()
            assertTrue(broken.isCompletedExceptionally, "initialisation returned after the run")
            assertEquals(1, proxyShutdowns.get())
        } finally { runtime.stop() }
    }

    @Test fun `on Velocity a plugin failing before its scripts ran shuts the proxy down after the others migrated`(@TempDir directory: Path) {
        val runtime = proxy(directory)
        try {
            val needs = register("needs", unmet)
            val zother = register("zother", other)
            runtime.initialized()
            assertTrue(needs.isCompletedExceptionally)
            assertEquals(1, zother.getNow(null))
            assertEquals(1, proxyShutdowns.get())
        } finally { runtime.stop() }
    }

    @Test fun `on Velocity a clean startup run lets initialisation finish`(@TempDir directory: Path) {
        val runtime = proxy(directory)
        try {
            val zother = register("zother", other)
            runtime.initialized()
            assertEquals(1, zother.getNow(null))
            assertEquals(0, proxyShutdowns.get())
        } finally { runtime.stop() }
    }
}
