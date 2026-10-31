package com.sneakydb

internal class CmiMaintenance(
    private val enabled: () -> Boolean,
    private val dispatch: (String) -> Boolean,
    private val severe: (String) -> Unit,
) {
    fun activate(command: String) {
        if (!enabled()) severe("Database unavailable: CMI is missing or disabled; server is NOT protected by maintenance. Close access manually.")
        else if (!try { dispatch(command) } catch (_: Exception) { false }) {
            severe("Database unavailable: CMI maintenance command failed; server is NOT protected. Close access manually.")
        }
    }
}
