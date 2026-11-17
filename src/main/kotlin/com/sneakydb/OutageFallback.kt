package com.sneakydb

import java.util.concurrent.atomic.AtomicBoolean

/**
 * Sends `cmi maintenance true <message>` at most once per enable generation. [startupFailed] may be called from any
 * thread and sends it on the server thread through [onMain]; [activateNow] sends it at once from the server thread.
 */
internal class OutageFallback(
    message: String,
    private val onMain: (() -> Unit) -> Unit,
    private val command: (String) -> Unit,
) {
    private val message = playerMessage(message, "Database unavailable. Please try again later.")
    private val stopped = AtomicBoolean()
    /** A send is already scheduled on the server thread (or was sent). */
    private val requested = AtomicBoolean()
    private val sent = AtomicBoolean()

    fun close() { stopped.set(true) }

    fun startupFailed() {
        if (!stopped.get() && requested.compareAndSet(false, true)) onMain { send() }
    }

    /**
     * For a caller on the server thread that must not let startup continue before maintenance is on. A send scheduled
     * earlier by [startupFailed] then does nothing.
     */
    fun activateNow() {
        requested.set(true)
        send()
    }

    private fun send() {
        if (!stopped.get() && sent.compareAndSet(false, true)) command("cmi maintenance true $message")
    }
}
