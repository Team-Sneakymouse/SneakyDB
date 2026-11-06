package com.sneakydb

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.function.Consumer

class NotificationWorkerTest {
    class FakeConnection : NotificationConnection {
        val listened = mutableListOf<String>()
        val unlistened = mutableListOf<String>()
        val pending = ArrayDeque<Pair<String, String>>()
        var failPoll = false
        var failPollWith: Throwable? = null
        var valid = true
        var closed = false
        var checks = 0
        var onListen: (String) -> Unit = {}
        override fun listen(channel: String) { listened.add(channel); onListen(channel) }
        override fun unlisten(channel: String) { unlistened.add(channel) }
        override fun poll(timeoutMs: Int): List<Pair<String, String>> {
            failPollWith?.let { throw it }
            if (failPoll) throw java.sql.SQLException("secret driver detail")
            return pending.toList().also { pending.clear() }
        }
        override fun check() { checks++; check(valid) }
        override fun close() { closed = true }
    }

    private val warnings = mutableListOf<String>()
    private val connections = mutableListOf<FakeConnection>()
    private var openFails = false
    private var now = 0L
    private var threadStarts = 0

    // No real thread: each test drives the loop with tick().
    private val worker = NotificationWorker(
        open = { if (openFails) error("secret") else FakeConnection().also { connections.add(it) } },
        warn = { warnings.add(it) },
        keepAliveMs = 1000,
        clock = { now },
        startThread = { threadStarts++; null },
    )

    private fun subscribe(channel: String, onNotification: (String) -> Unit = {}, onConnected: () -> Unit = {}) =
        worker.subscribe(channel, Consumer { onNotification(it) }, Runnable { onConnected() })

    @Test fun `opens nothing until the first subscription and starts one thread`() {
        assertEquals(0, threadStarts)
        subscribe("pushes")
        subscribe("other")
        assertEquals(1, threadStarts)
        assertTrue(connections.isEmpty())
    }

    @Test fun `listens, announces once and delivers payloads by channel`() {
        val pushes = mutableListOf<String>()
        val other = mutableListOf<String>()
        var connected = 0
        subscribe("pushes", { pushes.add(it) }, { connected++ })
        subscribe("other", { other.add(it) })
        worker.tick()
        val connection = connections.single()
        assertEquals(setOf("pushes", "other"), connection.listened.toSet())
        connection.pending.add("pushes" to "42")
        worker.tick()
        worker.tick()
        assertEquals(listOf("42"), pushes)
        assertEquals(emptyList<String>(), other)
        assertEquals(1, connected)
    }

    @Test fun `a subscription added during LISTEN is announced only once its own channel listens`() {
        subscribe("pushes")
        worker.tick()
        var lateConnected = 0
        connections.single().onListen = { channel ->
            if (channel == "other") subscribe("late", onConnected = { lateConnected++ })
        }
        subscribe("other")
        worker.tick()
        assertEquals(0, lateConnected, "not before LISTEN late")
        assertFalse("late" in connections.single().listened)
        worker.tick()
        assertTrue("late" in connections.single().listened)
        assertEquals(1, lateConnected)
    }

    @Test fun `a subscription gets no notification before its first onConnected`() {
        val events = mutableListOf<String>()
        subscribe("pushes", { if (it == "1") subscribe("pushes", { p -> events.add("notified $p") }, { events.add("connected") }) })
        worker.tick()
        connections.single().pending.addAll(listOf("pushes" to "1", "pushes" to "2"))
        worker.tick()
        connections.single().pending.add("pushes" to "3")
        worker.tick()
        assertEquals(listOf("connected", "notified 3"), events)
    }

    @Test fun `callback errors such as AssertionError are contained`() {
        val delivered = mutableListOf<String>()
        subscribe("pushes", { throw AssertionError("secret") }, { throw NoClassDefFoundError("secret") })
        subscribe("pushes", { delivered.add(it) })
        worker.tick()
        connections.single().pending.add("pushes" to "1")
        worker.tick()
        assertEquals(listOf("1"), delivered)
        assertEquals(2, warnings.size)
        assertTrue(warnings.none { "secret" in it })
    }

    @Test fun `a loop ended by a VM error closes its connection and the next subscription starts a new one`() {
        var loop: Runnable? = null
        val errors = NotificationWorker(
            open = { FakeConnection().also { connections.add(it) } },
            warn = { warnings.add(it) },
            startThread = { loop = it; threadStarts++; null },
        )
        errors.subscribe("pushes", Consumer {}, Runnable {})
        // The first tick opens the connection; then its poll fails with an Error the loop doesn't catch.
        errors.tick()
        connections.single().failPollWith = StackOverflowError()
        assertThrows(StackOverflowError::class.java) { loop!!.run() }
        assertTrue(connections.single().closed)
        errors.subscribe("other", Consumer {}, Runnable {})
        assertEquals(2, threadStarts)
        errors.close()
    }

    @Test fun `a later subscription is listened and announced on the open connection`() {
        subscribe("pushes")
        worker.tick()
        var connected = 0
        subscribe("later", onConnected = { connected++ })
        worker.tick()
        assertEquals(listOf("pushes", "later"), connections.single().listened)
        assertEquals(1, connected)
    }

    @Test fun `reconnects after a failure, announces again and warns once without driver details`() {
        val connected = mutableListOf<Int>()
        subscribe("pushes", onConnected = { connected.add(connections.size) })
        assertEquals(0, worker.tick())
        connections.single().failPoll = true
        assertEquals(1000, worker.tick())
        assertTrue(connections.single().closed)
        openFails = true
        assertEquals(2000, worker.tick())
        assertEquals(4000, worker.tick())
        openFails = false
        assertEquals(0, worker.tick())
        assertEquals(listOf(1, 2), connected)
        assertEquals(listOf("pushes"), connections[1].listened)
        assertEquals(listOf("SneakyDB notification connection lost; reconnecting"), warnings)
        // Recovery resets the backoff and the one warning per outage.
        connections[1].failPoll = true
        assertEquals(1000, worker.tick())
        assertEquals(2, warnings.size)
    }

    @Test fun `backoff doubles up to thirty seconds`() {
        openFails = true
        subscribe("pushes")
        assertEquals(listOf(1000L, 2000, 4000, 8000, 16000, 30000, 30000, 30000), (1..8).map { worker.tick() })
        assertEquals(listOf("SneakyDB notification connection failed; retrying"), warnings)
    }

    @Test fun `unsubscribing the last listener of a channel unlistens it`() {
        val first = subscribe("pushes")
        val second = subscribe("pushes")
        worker.tick()
        first.close()
        worker.tick()
        assertEquals(emptyList<String>(), connections.single().unlistened)
        second.close()
        worker.tick()
        assertEquals(listOf("pushes"), connections.single().unlistened)
    }

    @Test fun `a closed subscription receives nothing more`() {
        val received = mutableListOf<String>()
        val subscription = subscribe("pushes", { received.add(it) })
        worker.tick()
        subscription.close()
        connections.single().pending.add("pushes" to "1")
        worker.tick()
        assertTrue(received.isEmpty())
    }

    @Test fun `keep-alive check runs on its interval and a dead connection reconnects`() {
        subscribe("pushes")
        worker.tick()
        now = 999
        worker.tick()
        assertEquals(0, connections.single().checks)
        now = 1000
        connections.single().valid = false
        assertEquals(1000, worker.tick())
        assertEquals(1, connections.single().checks)
        worker.tick()
        assertEquals(2, connections.size)
    }

    @Test fun `a failing callback is contained and reported by channel only`() {
        val delivered = mutableListOf<String>()
        subscribe("pushes", { error("secret consumer detail") })
        subscribe("pushes", { delivered.add(it) })
        worker.tick()
        connections.single().pending.add("pushes" to "1")
        worker.tick()
        assertEquals(listOf("1"), delivered)
        assertEquals(listOf("SneakyDB notification listener for channel pushes failed"), warnings)
    }

    @Test fun `rejects channel names that would need quoting`() {
        listOf("", "Pushes", "a-b", "a\"; drop table x; --", "1abc", "a".repeat(64)).forEach {
            assertThrows(IllegalArgumentException::class.java) { subscribe(it) }
        }
        assertDoesNotThrow { subscribe("a".repeat(63)) }
    }

    @Test fun `close ends every subscription and refuses new ones`() {
        subscribe("pushes")
        worker.tick()
        worker.close()
        assertEquals(0, worker.tick())
        assertThrows(IllegalStateException::class.java) { subscribe("pushes") }
    }
}
