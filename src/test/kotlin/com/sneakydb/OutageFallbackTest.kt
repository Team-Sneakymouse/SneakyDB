package com.sneakydb

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class OutageFallbackTest {
    @Test fun `unsafe or blank message uses nonempty single line default`() {
        for (message in listOf("", "   ", "offline\ncommand", "offline\rcommand", "x".repeat(513))) {
            val commands = mutableListOf<String>()
            OutageFallback(message, { it() }, { commands.add(it) }).startupFailed()
            assertEquals(listOf("cmi maintenance true Database unavailable. Please try again later."), commands)
        }
    }

    @Test fun `closed generation cannot dispatch queued or later outages`() {
        val main = ArrayDeque<() -> Unit>()
        val commands = mutableListOf<String>()
        val fallback = OutageFallback("Offline", { main.add(it) }, { commands.add(it) })
        fallback.startupFailed()
        fallback.close()
        while (main.isNotEmpty()) main.removeFirst()()
        fallback.startupFailed()
        assertTrue(commands.isEmpty())
    }

    @Test fun `repeated outages activate only once`() {
        val main = ArrayDeque<() -> Unit>()
        val commands = mutableListOf<String>()
        val fallback = OutageFallback("Offline", { main.add(it) }, { commands.add(it) })
        repeat(3) { fallback.startupFailed() }
        while (main.isNotEmpty()) main.removeFirst()()
        assertEquals(1, commands.size)
    }

    @Test fun `startup failure queues explicit maintenance on server thread`() {
        val main = ArrayDeque<() -> Unit>()
        val commands = mutableListOf<String>()
        val fallback = OutageFallback("Database unavailable", { main.add(it) }, { commands.add(it) })
        fallback.startupFailed()
        assertTrue(commands.isEmpty())
        main.removeFirst()()
        assertEquals(listOf("cmi maintenance true Database unavailable"), commands)
    }
}
