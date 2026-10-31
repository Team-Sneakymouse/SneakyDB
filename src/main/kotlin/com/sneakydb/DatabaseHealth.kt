package com.sneakydb

import java.util.concurrent.CompletableFuture

/** Health is separate from the historical consumer readiness contract. */
internal class DatabaseHealth(
    ready: CompletableFuture<Void>,
    private val fallback: OutageFallback,
    probe: () -> Boolean,
    schedule: (() -> Unit) -> AutoCloseable,
) : AutoCloseable {
    private val lock = Any()
    private var stopped = false
    private var task: AutoCloseable? = null
    init {
        ready.whenComplete { _, failure ->
            synchronized(lock) {
                if (!stopped) {
                    if (failure != null) fallback.startupFailed()
                    else task = schedule {
                        val admitted = synchronized(lock) { !stopped }
                        if (admitted) {
                            val healthy = try { probe() } catch (_: Exception) { false }
                            synchronized(lock) { if (!stopped && !healthy) fallback.startupFailed() }
                        }
                    }
                }
            }
        }
    }
    override fun close() {
        synchronized(lock) {
            if (stopped) return
            stopped = true
            fallback.close()
            task?.close()
            task = null
        }
    }
}
