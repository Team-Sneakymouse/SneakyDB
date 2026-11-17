package com.sneakydb

import com.sneakydb.MigrationScriptsTest.Companion.scripts
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** What a server decides under the migration lock, and what counts as present when it plans. */
class MigrationRecheckTest {
    private val alpha = scripts("alpha", "V1__a.sql" to "select 1;", "V2__b.sql" to "select 2;")
    private fun applied(vararg versions: Int) = versions.map { AppliedVersion(it, alpha[it - 1].checksum) }
    private fun step(version: Int, requirements: List<Requirement> = emptyList(), absent: List<Requirement> = emptyList()) =
        PlannedStep("alpha", alpha[version - 1], alpha, requirements, absent)

    private fun refusal(block: () -> Unit): String = assertThrows(MigrationException::class.java) { block() }.message!!

    @Test fun `a step runs when the database is where the plan expected it`() {
        assertTrue(recheckStep(step(2), mapOf("alpha" to applied(1))))
    }

    @Test fun `a step another server applied is skipped only while the plugin's history still matches its jar`() {
        assertFalse(recheckStep(step(1), mapOf("alpha" to applied(1, 2))))
        assertFalse(recheckStep(step(1), mapOf("alpha" to listOf(AppliedVersion(1, null)))), "an operator row is not compared")
        assertTrue(refusal { recheckStep(step(1), mapOf("alpha" to applied(1, 2) + AppliedVersion(3, "x"))) }
            .startsWith("the database is newer than this build: it is at version 3, this build's scripts go up to 2"))
        assertTrue(refusal { recheckStep(step(2), mapOf("alpha" to listOf(AppliedVersion(1, "0".repeat(64))))) }
            .contains("V1__a.sql differs from the script that was applied (checksum mismatch)"))
    }

    @Test fun `requirements are checked again, and a plugin that was absent must still be absent or new enough`() {
        val needsBeta = Requirement("beta", 2, false)
        assertTrue(refusal { recheckStep(step(1, requirements = listOf(needsBeta)), mapOf("beta" to listOf(AppliedVersion(1, null)))) }
            .contains("it needs requires: beta >= 2, but the database has beta at version 1; restart the server"))
        val ifMeta = Requirement("meta", 2, true)
        assertTrue(recheckStep(step(1, absent = listOf(ifMeta)), emptyMap()))
        assertTrue(recheckStep(step(1, absent = listOf(ifMeta)), mapOf("meta" to listOf(AppliedVersion(1, null), AppliedVersion(2, null)))))
        assertTrue(refusal { recheckStep(step(1, absent = listOf(ifMeta)), mapOf("meta" to listOf(AppliedVersion(1, null)))) }
            .contains("meta had no rows when this server planned its migrations, but another server has since applied its scripts up to version 1"))
    }

    @Test fun `a checksum fill-in compares what another server filled in meanwhile`() {
        assertEquals(listOf(1), recheckFill("alpha", alpha, mapOf("alpha" to listOf(AppliedVersion(1, null)))))
        assertEquals(emptyList<Int>(), recheckFill("alpha", alpha, mapOf("alpha" to applied(1))), "the same script filled it: nothing to do")
        assertTrue(refusal { recheckFill("alpha", alpha, mapOf("alpha" to listOf(AppliedVersion(1, "0".repeat(64))))) }.contains("checksum mismatch"))
    }

    @Test fun `a plugin is ready only when its rows are exactly its jar's scripts`() {
        recheckReady("alpha", alpha, mapOf("alpha" to applied(1, 2)))
        assertTrue(refusal { recheckReady("alpha", alpha, mapOf("alpha" to applied(1))) }.contains("expected alpha at version 2, but the database has version 1"))
        assertTrue(refusal { recheckReady("alpha", alpha, mapOf("alpha" to applied(1, 2) + AppliedVersion(3, null))) }.contains("newer than this build"))
    }

    @Test fun `plugins that registered earlier in this generation count as present, and their failure is named`() {
        val optional = mapOf("optional" to Result.success(scripts("optional", "V1__o.sql" to "-- requires-if-present: broken >= 1\nselect 1;")))
        val ignored = planMigrations(optional, emptyMap(), MigrationMode.APPLY)
        assertEquals(listOf(Requirement("broken", 1, true)), ignored.steps.single().absent)

        val failed = planMigrations(optional, emptyMap(), MigrationMode.APPLY, earlier = mapOf("broken" to "Migrations for broken failed"))
        assertTrue(failed.failures.getValue("optional").reason.contains("broken's migrations failed earlier on this server"))

        val tooOld = planMigrations(optional.mapValues { Result.success(scripts("optional", "V1__o.sql" to "-- requires: done >= 2\nselect 1;")) },
            mapOf("done" to listOf(AppliedVersion(1, null))), MigrationMode.APPLY, earlier = mapOf("done" to null))
        assertTrue(tooOld.failures.getValue("optional").reason.contains("done's build on this server goes no higher"))
    }

    @Test fun `the scanner reports the line and words of the first transaction control`() {
        assertEquals(3 to "COMMIT", findTransactionControl("select 1;\n\n  commit;\nbegin;"))
        assertEquals(null, findTransactionControl("do $$ begin commit; end $$;"))
        assertEquals(1 to "START TRANSACTION", findTransactionControl("START /* why */ TRANSACTION"))
        assertEquals(null, findTransactionControl("start replication;"))
        assertEquals(null, findTransactionControl("prepare plan as select 1;"))
    }
}
