package com.sneakydb

internal class OutageFallback(
    message: String,
    private val onMain: (() -> Unit) -> Unit,
    private val command: (String) -> Unit,
) {
    private val message = message.takeIf {
        it.isNotBlank() && it.length <= 512 && it.none(Char::isISOControl)
    } ?: "Database unavailable. Please try again later."
    private val stopped = java.util.concurrent.atomic.AtomicBoolean()
    fun close() { stopped.set(true) }
    private val activated = java.util.concurrent.atomic.AtomicBoolean()
    fun startupFailed() {
        if (!stopped.get() && activated.compareAndSet(false, true)) onMain {
            if (!stopped.get()) command("cmi maintenance true $message")
        }
    }
}
