package com.sneakydb

import java.util.concurrent.CopyOnWriteArrayList
import java.util.function.Consumer

/** A connection that receives notifications. Only the notification thread touches it. */
internal interface NotificationConnection : AutoCloseable {
    fun listen(channel: String)
    fun unlisten(channel: String)

    /** Waits up to [timeoutMs] and returns the (channel, payload) pairs that arrived. */
    fun poll(timeoutMs: Int): List<Pair<String, String>>

    /** Throws when the connection is no longer usable. */
    fun check()
}

/** Lower-case PostgreSQL identifiers only, so they can be quoted into LISTEN without escaping. */
internal val CHANNEL_NAME = Regex("[a-z_][a-z0-9_]{0,62}")

/**
 * Owns the one notification connection and every subscription of an enable generation.
 * The connection opens with the first subscription and reconnects with backoff after a failure.
 * Callbacks run on the notification thread. Logs must stay credential-safe: never log driver messages.
 */
internal class NotificationWorker(
    private val open: () -> NotificationConnection,
    private val warn: (String) -> Unit,
    private val pollMs: Int = 500,
    private val keepAliveMs: Long = 30_000,
    private val clock: () -> Long = System::currentTimeMillis,
    private val startThread: (Runnable) -> Thread? = { loop ->
        Thread(loop, "SneakyDB-Notifications").apply { isDaemon = true; start() }
    },
) : AutoCloseable {
    internal class Subscription(
        val channel: String,
        val onNotification: Consumer<String>,
        val onConnected: Runnable,
    ) {
        @Volatile var closed = false

        /** The connection generation this subscription was last announced on. */
        var announced = 0L
    }

    private val subscriptions = CopyOnWriteArrayList<Subscription>()
    private val lock = Any()
    private var thread: Thread? = null
    private var started = false
    @Volatile private var closed = false

    // Owned by the notification thread (or a test calling tick()).
    private var connection: NotificationConnection? = null
    private var generation = 0L
    private val listened = HashSet<String>()
    private var failures = 0
    private var lastCheck = 0L

    fun subscribe(channel: String, onNotification: Consumer<String>, onConnected: Runnable): AutoCloseable {
        require(CHANNEL_NAME.matches(channel)) { "Channel names must match ${CHANNEL_NAME.pattern}" }
        val subscription = Subscription(channel, onNotification, onConnected)
        synchronized(lock) {
            check(!closed) { "SneakyDB has shut down" }
            subscriptions.add(subscription)
            if (!started) { started = true; thread = startThread(Runnable(::run)) }
        }
        return AutoCloseable {
            subscription.closed = true
            subscriptions.remove(subscription)
        }
    }

    private fun run() {
        try {
            while (!closed) {
                val wait = tick()
                if (wait > 0) try { Thread.sleep(wait) } catch (_: InterruptedException) { /* closing */ }
            }
        } finally {
            // Also when an Error ends the loop: close the connection and let the next subscription start a new thread.
            closeConnection()
            synchronized(lock) { if (!closed) { started = false; thread = null } }
        }
    }

    /** Runs one step and returns how long to wait before the next one. */
    internal fun tick(): Long {
        if (closed) return 0
        val current = connection ?: try {
            open().also { connection = it; generation++; listened.clear(); lastCheck = clock() }
        } catch (_: Exception) {
            return fail("SneakyDB notification connection failed; retrying")
        }
        return try {
            sync(current)
            current.poll(pollMs).forEach { (channel, payload) -> dispatch(channel, payload) }
            if (clock() - lastCheck >= keepAliveMs) { current.check(); lastCheck = clock() }
            failures = 0
            0
        } catch (_: Exception) {
            closeConnection()
            fail("SneakyDB notification connection lost; reconnecting")
        }
    }

    private fun fail(message: String): Long {
        if (failures == 0) warn(message)
        return (1000L shl failures.coerceAtMost(5)).coerceAtMost(30_000).also { failures++ }
    }

    private fun sync(current: NotificationConnection) {
        // One snapshot for both steps: a subscription added meanwhile waits for the next tick, so onConnected never
        // runs before its channel listens (its re-read could otherwise miss a change made in between).
        val snapshot = subscriptions.toList()
        val wanted = snapshot.mapTo(HashSet()) { it.channel }
        (wanted - listened).forEach { current.listen(it); listened.add(it) }
        (listened - wanted).forEach { current.unlisten(it); listened.remove(it) }
        for (subscription in snapshot) {
            if (subscription.announced == generation || subscription.closed || subscription.channel !in listened) continue
            subscription.announced = generation
            guard(subscription.channel) { subscription.onConnected.run() }
        }
    }

    private fun dispatch(channel: String, payload: String) {
        for (subscription in subscriptions) {
            // Only after its onConnected on this connection: its first re-read comes before any notification.
            if (subscription.channel != channel || subscription.closed || subscription.announced != generation) continue
            guard(channel) { subscription.onNotification.accept(payload) }
        }
    }

    /** Contains everything a callback throws (AssertionError and NoClassDefFoundError included) except VM errors. */
    private fun guard(channel: String, callback: () -> Unit) {
        try {
            callback()
        } catch (error: VirtualMachineError) {
            throw error
        } catch (_: Throwable) {
            warn("SneakyDB notification listener for channel $channel failed")
        }
    }

    private fun closeConnection() {
        connection?.let { runCatching { it.close() } }
        connection = null
    }

    /** Stops the thread without waiting for it; the connection closes on that thread. */
    override fun close() {
        synchronized(lock) {
            closed = true
            subscriptions.forEach { it.closed = true }
            subscriptions.clear()
            thread?.interrupt()
        }
    }
}
