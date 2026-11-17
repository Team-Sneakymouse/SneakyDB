package com.sneakydb

import com.sneakydb.MigrationScriptsTest.Companion.scripts
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class MigrationPlanTest {
    /** `script("-- requires: b >= 1")` for each argument: version 1, 2, … of [plugin]. */
    private fun plugin(plugin: String, vararg bodies: String) =
        plugin to Result.success(scripts(plugin, *bodies.mapIndexed { i, body -> "V${i + 1}__s${i + 1}.sql" to "$body\nselect 1;" }.toTypedArray()))

    private fun applied(vararg versions: Int) = versions.map { AppliedVersion(it, "x") }

    private fun plan(
        vararg registered: Pair<String, Result<List<MigrationScript>>>,
        applied: Map<String, List<AppliedVersion>> = emptyMap(),
        mode: MigrationMode = MigrationMode.APPLY,
    ): MigrationPlan {
        // Rows recorded by SneakyDB carry the jar's checksum unless a test says otherwise.
        val withChecksums = applied.mapValues { (name, rows) ->
            val own = registered.toMap()[name]?.getOrNull()
            rows.map { row -> if (row.checksum == "x") AppliedVersion(row.version, own?.getOrNull(row.version - 1)?.checksum ?: "x") else row }
        }
        return planMigrations(registered.toMap(), withChecksums, mode)
    }

    private val MigrationPlan.order get() = steps.map { it.toString() }

    /** The interleaved change of one shared relation: alpha drops the key, beta retypes the column, alpha re-adds the key. */
    private val alpha = plugin("alpha", "", "-- requires: beta >= 1", "-- requires: beta >= 2")
    private val beta = plugin("beta", "-- requires: alpha >= 1", "-- requires: alpha >= 2")

    @Test fun `independent plugins run in plugin id then version order`() {
        val plan = plan(plugin("zeta", "", ""), plugin("alpha", "", ""), plugin("mid", ""))
        assertEquals(listOf("alpha:V1__s1.sql", "alpha:V2__s2.sql", "mid:V1__s1.sql", "zeta:V1__s1.sql", "zeta:V2__s2.sql"), plan.order)
        assertEquals(mapOf("alpha" to 2, "mid" to 1, "zeta" to 2), plan.targetVersions)
        assertTrue(plan.failures.isEmpty())
    }

    @Test fun `only scripts above the database version are pending`() {
        val plan = plan(plugin("alpha", "", "", ""), applied = mapOf("alpha" to applied(1, 2)))
        assertEquals(listOf("alpha:V3__s3.sql"), plan.order)
        val current = plan(plugin("alpha", "", ""), applied = mapOf("alpha" to applied(1, 2)))
        assertEquals(emptyList<String>(), current.order)
        assertEquals(mapOf("alpha" to 2), current.targetVersions)
    }

    @Test fun `interleaved scripts of two plugins run in the order their requirements give`() {
        val plan = plan(beta, alpha)
        assertEquals(listOf("alpha:V1__s1.sql", "beta:V1__s1.sql", "alpha:V2__s2.sql", "beta:V2__s2.sql", "alpha:V3__s3.sql"), plan.order)
        assertTrue(plan.failures.isEmpty())
        assertEquals(listOf(StepKey("alpha", 2), StepKey("beta", 2)), plan.dependents[StepKey("beta", 1)]?.sorted())
    }

    @Test fun `merging only one side of an interleaved change runs nothing in that group`() {
        val onlyAlpha = plan(alpha, plugin("beta", "-- requires: alpha >= 1"), plugin("other", ""), applied = mapOf("alpha" to applied(1), "beta" to applied(1)))
        assertEquals(listOf("other:V1__s1.sql"), onlyAlpha.order)
        assertEquals(setOf("alpha"), onlyAlpha.failures.keys)
        assertEquals("V3__s3.sql", onlyAlpha.failures.getValue("alpha").script)
        assertTrue(onlyAlpha.failures.getValue("alpha").reason.contains("needs requires: beta >= 2, but the database has beta at version 1 and its build on this server only has scripts up to 1"))
        assertEquals(mapOf("beta" to 1, "other" to 1), onlyAlpha.targetVersions)

        val onlyBeta = plan(plugin("alpha", ""), beta, applied = mapOf("alpha" to applied(1), "beta" to applied(1)))
        assertEquals(emptyList<String>(), onlyBeta.order)
        assertEquals(setOf("beta"), onlyBeta.failures.keys)
        assertTrue(onlyBeta.failures.getValue("beta").reason.contains("needs requires: alpha >= 2"))
    }

    @Test fun `a cycle fails every plugin of its group and nothing else`() {
        val plan = plan(plugin("alpha", "-- requires: beta >= 1"), plugin("beta", "-- requires: alpha >= 1"), plugin("gamma", ""))
        assertEquals(listOf("gamma:V1__s1.sql"), plan.order)
        assertEquals(setOf("alpha", "beta"), plan.failures.keys)
        plan.failures.values.forEach { assertTrue(it.reason.contains("form a cycle"), it.reason) }
        assertTrue(plan.failures.getValue("alpha").reason.contains("alpha:V1__s1.sql → beta:V1__s1.sql → alpha:V1__s1.sql"))
    }

    @Test fun `a cycle through a plugin's own chain is found`() {
        // alpha 2 needs beta 1, which needs alpha 3, which needs alpha 2.
        val plan = plan(plugin("alpha", "", "-- requires: beta >= 1", ""), plugin("beta", "-- requires: alpha >= 3"))
        assertEquals(listOf<String>(), plan.order)
        assertEquals(setOf("alpha", "beta"), plan.failures.keys)
    }

    @Test fun `hard requirements need the database or a registered plugin to reach the version`() {
        val missing = plan(plugin("alpha", "-- requires: sneakycharacters >= 3"))
        assertTrue(missing.failures.getValue("alpha").reason.contains("sneakycharacters has not registered migrations on this server"))
        val satisfiedByDatabase = plan(plugin("alpha", "-- requires: sneakycharacters >= 3"), applied = mapOf("sneakycharacters" to applied(1, 2, 3)))
        assertEquals(listOf("alpha:V1__s1.sql"), satisfiedByDatabase.order)
        assertEquals(listOf(Requirement("sneakycharacters", 3, false)), satisfiedByDatabase.steps.single().requirements)
        val tooLow = plan(plugin("alpha", "-- requires: sneakycharacters >= 3"), applied = mapOf("sneakycharacters" to applied(1, 2)))
        assertTrue(tooLow.failures.getValue("alpha").reason.contains("the database has sneakycharacters at version 2"))
    }

    @Test fun `requires-if-present is ignored only when the other plugin is absent`() {
        val absent = plan(plugin("alpha", "-- requires-if-present: meta >= 5"))
        assertEquals(listOf("alpha:V1__s1.sql"), absent.order)
        assertEquals(emptyList<Requirement>(), absent.steps.single().requirements)

        val presentInDatabase = plan(plugin("alpha", "-- requires-if-present: meta >= 5"), applied = mapOf("meta" to applied(1, 2)))
        assertEquals(setOf("alpha"), presentInDatabase.failures.keys)

        val registeredWithScripts = plan(plugin("alpha", "-- requires-if-present: meta >= 2"), plugin("meta", "", ""))
        assertEquals(listOf("meta:V1__s1.sql", "meta:V2__s2.sql", "alpha:V1__s1.sql"), registeredWithScripts.order)

        val registeredTooOld = plan(plugin("alpha", "-- requires-if-present: meta >= 2"), plugin("meta", ""))
        assertEquals(setOf("alpha", "meta"), registeredTooOld.failures.keys, "meta's pending script is in alpha's group")
    }

    @Test fun `a group with an unresolvable requirement fails as a whole, other groups still run`() {
        // beta needs alpha 2, alpha 2 needs a plugin that is not there: both fail; gamma runs.
        val plan = plan(plugin("alpha", "", "-- requires: missing >= 1"), plugin("beta", "-- requires: alpha >= 2"), plugin("gamma", ""))
        assertEquals(listOf("gamma:V1__s1.sql"), plan.order)
        assertEquals(setOf("alpha", "beta"), plan.failures.keys)
        assertTrue(plan.failures.getValue("beta").reason.startsWith("not run because scripts it is grouped with by requires lines can't run"))
        assertEquals(mapOf("gamma" to 1), plan.targetVersions)
    }

    @Test fun `an unresolvable requirement stops its whole group, including the plugin it names`() {
        // alpha 2 needs beta 3, but beta only ships 1 and 2: beta's scripts don't run either.
        val plan = plan(plugin("alpha", "", "-- requires: beta >= 3"), plugin("beta", "", ""), plugin("gamma", ""))
        assertEquals(listOf("gamma:V1__s1.sql"), plan.order)
        assertEquals(setOf("alpha", "beta"), plan.failures.keys)
        assertTrue(plan.failures.getValue("alpha").reason.contains("needs requires: beta >= 3"))
        assertTrue(plan.failures.getValue("beta").reason.startsWith("not run because scripts it is grouped with by requires lines can't run"))
        assertEquals(mapOf("gamma" to 1), plan.targetVersions)
    }

    @Test fun `requiring a plugin whose checks failed is unresolvable`() {
        val broken = "broken" to Result.failure<List<MigrationScript>>(MigrationException("broken: bad name"))
        val plan = plan(broken, plugin("alpha", "-- requires: broken >= 1"))
        assertEquals("broken: bad name", plan.failures.getValue("broken").reason)
        assertTrue(plan.failures.getValue("alpha").reason.contains("broken's migrations failed"))
    }

    @Test fun `a database newer than the build fails the plugin`() {
        val plan = plan(plugin("alpha", "", ""), applied = mapOf("alpha" to applied(1, 2, 3)))
        assertTrue(plan.failures.getValue("alpha").reason.startsWith("the database is newer than this build: it is at version 3, this build's scripts go up to 2"))
        assertTrue(plan.order.isEmpty())
    }

    @Test fun `version rows must be contiguous from 1`() {
        val plan = plan(plugin("alpha", "", "", ""), applied = mapOf("alpha" to applied(1, 3)))
        assertTrue(plan.failures.getValue("alpha").reason.contains("not contiguous from 1"))
    }

    @Test fun `an edited script fails its plugin, a null checksum is filled in apply mode only`() {
        val edited = plan(plugin("alpha", "", ""), applied = mapOf("alpha" to listOf(AppliedVersion(1, "0".repeat(64)))))
        assertEquals("V1__s1.sql", edited.failures.getValue("alpha").script)
        assertTrue(edited.failures.getValue("alpha").reason.contains("checksum mismatch"))

        val alphaScripts = plugin("alpha", "", "")
        val operator = plan(alphaScripts, applied = mapOf("alpha" to listOf(AppliedVersion(1, null))))
        assertEquals(listOf("alpha"), operator.checksumFills)
        assertEquals(listOf("alpha:V2__s2.sql"), operator.order)

        val verify = plan(alphaScripts, applied = mapOf("alpha" to listOf(AppliedVersion(1, null), AppliedVersion(2, null))), mode = MigrationMode.VERIFY)
        assertTrue(verify.checksumFills.isEmpty())
        assertTrue(verify.failures.isEmpty())
        assertEquals(mapOf("alpha" to 2), verify.targetVersions)
    }

    @Test fun `verify mode fails plugins with pending scripts and plans nothing`() {
        val plan = plan(plugin("alpha", "", ""), plugin("beta", ""), applied = mapOf("alpha" to applied(1), "beta" to applied(1)), mode = MigrationMode.VERIFY)
        assertTrue(plan.order.isEmpty())
        assertEquals(setOf("alpha"), plan.failures.keys)
        assertEquals("V2__s2.sql", plan.failures.getValue("alpha").script)
        assertTrue(plan.failures.getValue("alpha").reason.contains("verify mode"))
        assertEquals(mapOf("beta" to 1), plan.targetVersions)
    }
}
