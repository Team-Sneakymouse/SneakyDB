package com.sneakydb

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class CmiMaintenanceTest {
    @Test fun `rejected or throwing dispatch logs credential safe protection failure`() {
        for (throws in listOf(false, true)) {
            val warnings = mutableListOf<String>()
            val cmi = CmiMaintenance({ true }, { if (throws) error("password=secret") else false }, { warnings.add(it) })
            assertDoesNotThrow { cmi.activate("cmi maintenance true Offline") }
            assertEquals(1, warnings.size)
            assertTrue(warnings.single().contains("NOT protected"))
            assertFalse(warnings.single().contains("secret"))
        }
    }

    @Test fun `enabled CMI receives explicit enable command without toggle or disable`() {
        val commands = mutableListOf<String>()
        val cmi = CmiMaintenance({ true }, { commands.add(it); true }, { fail(it) })
        val fallback = OutageFallback("Offline", { it() }, cmi::activate)
        fallback.startupFailed()
        assertEquals(listOf("cmi maintenance true Offline"), commands)
    }

    @Test fun `missing CMI refuses dispatch and reports inability to protect server`() {
        val warnings = mutableListOf<String>()
        val commands = mutableListOf<String>()
        val cmi = CmiMaintenance({ false }, { commands.add(it); true }, { warnings.add(it) })
        cmi.activate("cmi maintenance true Offline")
        assertTrue(commands.isEmpty())
        assertEquals(1, warnings.size)
        assertTrue(warnings.single().contains("NOT protected"))
    }
}
