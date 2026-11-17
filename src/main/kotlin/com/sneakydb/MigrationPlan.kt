package com.sneakydb

import java.util.PriorityQueue

// Planning a migration run: pure logic over registered scripts and the version table, no database access.

internal enum class MigrationMode { APPLY, VERIFY }

/** One row of `sneakydb.schema_migrations`; a null [checksum] was recorded by an operator. */
internal class AppliedVersion(val version: Int, val checksum: String?)

internal data class StepKey(val plugin: String, val version: Int) : Comparable<StepKey> {
    override fun compareTo(other: StepKey) = compareValuesBy(this, other, StepKey::plugin, StepKey::version)
}

/**
 * A pending script. Under the lock it is re-checked against the version table: [pluginScripts] (the plugin's whole jar)
 * against the plugin's rows, [requirements] against the other plugins' versions, and [absent] (`requires-if-present`
 * lines dropped because the other plugin had no rows and had not registered) against the other plugins still being
 * absent or new enough.
 */
internal class PlannedStep(
    val plugin: String,
    val script: MigrationScript,
    val pluginScripts: List<MigrationScript>,
    val requirements: List<Requirement>,
    val absent: List<Requirement> = emptyList(),
) {
    val key get() = StepKey(plugin, script.version)
    override fun toString() = "$plugin:${script.fileName}"
}

/** Why a plugin's migrations failed; [script] names the script involved, when there is one. */
internal class PluginFailure(val script: String?, val reason: String)

internal class MigrationPlan(
    /** Plugins that fail before anything runs. */
    val failures: Map<String, PluginFailure>,
    /** Plugins with operator rows (no checksum) to fill in, sorted (apply mode only). */
    val checksumFills: List<String>,
    /** Every pending script of the plugins that didn't fail, in an order that satisfies all requirements. */
    val steps: List<PlannedStep>,
    /** For each step, the steps that require it directly. */
    val dependents: Map<StepKey, List<StepKey>>,
    /** The scripts of each plugin that didn't fail; its schema version is the highest one. */
    val scripts: Map<String, List<MigrationScript>>,
) {
    val targetVersions: Map<String, Int> get() = scripts.mapValues { it.value.size }
}

/**
 * Plans one run. [registered] maps each plugin of this batch to its scripts, or to the failure that discovering them
 * gave. [applied] holds the version table's rows for every plugin, registered or not. [earlier] holds the plugins that
 * registered earlier in this enable generation and are not in this batch, each with the reason its migrations failed
 * or null; they count as present for `requires-if-present`. [waiting] names those of them whose registration has not
 * finished (they registered after this batch was put together): their database version is not final yet. Ties in the
 * order go by plugin id, then version, so every server computes the same order.
 */
internal fun planMigrations(
    registered: Map<String, Result<List<MigrationScript>>>,
    applied: Map<String, List<AppliedVersion>>,
    mode: MigrationMode,
    earlier: Map<String, String?> = emptyMap(),
    waiting: Set<String> = emptySet(),
): MigrationPlan {
    val failures = sortedMapOf<String, PluginFailure>()
    val fills = sortedSetOf<String>()
    val scripts = sortedMapOf<String, List<MigrationScript>>()
    val databaseVersion = { plugin: String -> versionOf(applied, plugin) }

    for ((plugin, discovered) in registered.toSortedMap()) {
        val own = discovered.getOrElse { failures[plugin] = PluginFailure(null, it.message ?: it.javaClass.simpleName); continue }
        val failure = checkAgainstDatabase(plugin, own, applied[plugin].orEmpty(), mode)
        if (failure != null) { failures[plugin] = failure; continue }
        scripts[plugin] = own
        if (mode == MigrationMode.APPLY && applied[plugin].orEmpty().any { it.checksum == null }) fills.add(plugin)
    }

    // Graph of pending scripts: each depends on its plugin's previous pending script and on the scripts it requires.
    val steps = sortedMapOf<StepKey, PlannedStep>()
    val dependencies = HashMap<StepKey, MutableList<StepKey>>()
    val unresolved = HashMap<StepKey, String>()
    // Every unmet requirement between plugins, resolvable or not: they decide the groups. One the database already
    // satisfies imposes no order and links nothing.
    val links = mutableListOf<Pair<StepKey, String>>()
    for ((plugin, own) in scripts) {
        val current = databaseVersion(plugin)
        for (script in own.drop(current)) {
            val key = StepKey(plugin, script.version)
            val needs = mutableListOf<StepKey>()
            if (script.version - 1 > current) needs.add(StepKey(plugin, script.version - 1))
            val effective = mutableListOf<Requirement>()
            val absent = mutableListOf<Requirement>()
            for (requirement in script.requirements) {
                val other = requirement.plugin
                val otherVersion = databaseVersion(other)
                val present = other in registered || other in earlier || applied[other].orEmpty().isNotEmpty()
                if (requirement.ifPresent && !present) { absent.add(requirement); continue }
                effective.add(requirement)
                if (otherVersion >= requirement.version) continue
                links.add(key to other)
                val otherScripts = scripts[other]
                when {
                    otherScripts != null && otherScripts.size >= requirement.version -> needs.add(StepKey(other, requirement.version))
                    unresolved.containsKey(key) -> {}
                    other in failures -> unresolved[key] = "${script.fileName} needs $requirement, but $other's migrations failed"
                    other in waiting -> unresolved[key] = "${script.fileName} needs $requirement, but the database has $other at " +
                        "version $otherVersion and $other registered after this plugin's migrations were planned, so its scripts " +
                        "haven't run yet; restart the server so both are planned together"
                    otherScripts != null -> unresolved[key] = "${script.fileName} needs $requirement, but the database has $other at " +
                        "version $otherVersion and its build on this server only has scripts up to ${otherScripts.size}; " +
                        "install the $other build that adds that script together with this one"
                    other in earlier -> unresolved[key] = "${script.fileName} needs $requirement, but the database has $other at " +
                        "version $otherVersion and " + (earlier[other]?.let { "$other's migrations failed earlier on this server; fix that failure and restart the server" }
                            ?: "$other's build on this server goes no higher; install the $other build that adds that script together with this one")
                    else -> unresolved[key] = "${script.fileName} needs $requirement, but the database has $other at version " +
                        "$otherVersion and $other has not registered migrations on this server; install that $other build first"
                }
            }
            steps[key] = PlannedStep(plugin, script, own, effective, absent)
            dependencies[key] = needs
        }
    }

    // Unmet requirements connect scripts into groups, whether or not the required version is pending here; a group with
    // an unresolvable requirement or a cycle fails as a whole.
    val groups = UnionFind(steps.keys)
    dependencies.forEach { (step, needs) -> needs.forEach { groups.union(step, it) } }
    val firstPending = steps.keys.groupBy { it.plugin }.mapValues { it.value.min() }
    links.forEach { (step, other) -> firstPending[other]?.let { groups.union(step, it) } }
    val failedGroups = HashMap<StepKey, PluginFailure>()
    for ((step, reason) in unresolved.toSortedMap()) {
        val failure = PluginFailure(steps.getValue(step).script.fileName, reason)
        failures.putIfAbsent(step.plugin, failure)
        failedGroups.putIfAbsent(groups.find(step), failure)
    }
    var order = topologicalOrder(steps.keys.filter { groups.find(it) !in failedGroups }, dependencies)
    val leftover = steps.keys.filter { groups.find(it) !in failedGroups }.toSet() - order.toSet()
    for (step in leftover.sorted()) {
        if (groups.find(step) in failedGroups) continue
        val cycle = findCycle(step, leftover, dependencies)
        val description = "the requires lines of ${cycle.joinToString(" → ") { "${it.plugin}:${steps.getValue(it).script.fileName}" }} " +
            "form a cycle; remove one of them"
        cycle.dropLast(1).forEach { failures.putIfAbsent(it.plugin, PluginFailure(steps.getValue(it).script.fileName, description)) }
        failedGroups[groups.find(step)] = PluginFailure(steps.getValue(cycle.first()).script.fileName, description)
    }
    order = order.filter { groups.find(it) !in failedGroups }
    for (step in steps.keys) {
        val cause = failedGroups[groups.find(step)] ?: continue
        if (step.plugin in failures) continue
        failures[step.plugin] = PluginFailure(steps.getValue(step).script.fileName,
            "not run because scripts it is grouped with by requires lines can't run: ${cause.reason}")
    }

    val dependents = HashMap<StepKey, MutableList<StepKey>>()
    for (step in order) dependencies.getValue(step).forEach { dependents.getOrPut(it) { mutableListOf() }.add(step) }
    return MigrationPlan(
        failures = failures,
        checksumFills = fills.filter { it !in failures },
        steps = order.map { steps.getValue(it) },
        dependents = dependents,
        scripts = scripts.filterKeys { it !in failures },
    )
}

private fun versionOf(rows: Map<String, List<AppliedVersion>>, plugin: String) = rows[plugin].orEmpty().maxOfOrNull { it.version } ?: 0

/** Strict version, checksum and verify-mode checks for one plugin against its rows in the version table. */
private fun checkAgainstDatabase(
    plugin: String,
    scripts: List<MigrationScript>,
    rows: List<AppliedVersion>,
    mode: MigrationMode,
): PluginFailure? {
    checkHistory(plugin, scripts, rows)?.let { return it }
    if (mode == MigrationMode.VERIFY && rows.size < scripts.size) {
        val pending = scripts.drop(rows.size)
        return PluginFailure(pending.first().fileName, "${pending.joinToString { it.fileName }} not applied, and this server runs " +
            "migrations in verify mode; start the server with migrations.mode: apply on this database first, then restart this one")
    }
    return null
}

/**
 * [plugin]'s applied history against its jar: the rows are contiguous from 1, none is above the jar's highest script,
 * and every recorded checksum matches the jar's script (a null one, recorded by an operator, is not compared).
 */
internal fun checkHistory(plugin: String, scripts: List<MigrationScript>, rows: List<AppliedVersion>): PluginFailure? {
    val versions = rows.map { it.version }.sorted()
    if (versions != (1..versions.size).toList()) {
        return PluginFailure(null, "the version table has rows for versions $versions, which are not contiguous from 1; " +
            "fix $plugin's rows in sneakydb.schema_migrations")
    }
    if (versions.size > scripts.size) {
        return PluginFailure(null, "the database is newer than this build: it is at version ${versions.size}, this build's scripts " +
            "go up to ${scripts.size}; install the $plugin build that matches the database or a newer one")
    }
    rows.sortedBy { it.version }.firstOrNull { it.checksum != null && it.checksum != scripts[it.version - 1].checksum }?.let {
        val name = scripts[it.version - 1].fileName
        return PluginFailure(name, "$name differs from the script that was applied (checksum mismatch); a merged script must never " +
            "be edited: restore the original and put the change in a new script")
    }
    return null
}

// What a server decides under the migration lock, from every row of the version table as it is then. Pure, so the
// PostgreSQL store and the tests' in-memory store share it. Each throws a MigrationException when the database no
// longer matches what this server planned against.

/** Returns true to run [step], false when another server applied it meanwhile. */
internal fun recheckStep(step: PlannedStep, rows: Map<String, List<AppliedVersion>>): Boolean {
    val own = rows[step.plugin].orEmpty()
    checkHistory(step.plugin, step.pluginScripts, own)?.let { throw MigrationException(it.reason) }
    val current = own.size
    if (current >= step.script.version) return false
    if (current != step.script.version - 1) throw MigrationException(
        "expected ${step.plugin} at version ${step.script.version - 1} before this script, but the database has version $current; restart the server")
    for (requirement in step.requirements) {
        val version = versionOf(rows, requirement.plugin)
        if (version < requirement.version) throw MigrationException(
            "it needs $requirement, but the database has ${requirement.plugin} at version $version; restart the server")
    }
    for (requirement in step.absent) {
        val version = versionOf(rows, requirement.plugin)
        if (version in 1 until requirement.version) throw MigrationException(
            "it has $requirement, and ${requirement.plugin} had no rows when this server planned its migrations, but another server " +
                "has since applied its scripts up to version $version; restart the server so it plans again")
    }
    return true
}

/** Returns the versions of [plugin]'s rows that still have no checksum, after checking its history against [scripts]. */
internal fun recheckFill(plugin: String, scripts: List<MigrationScript>, rows: Map<String, List<AppliedVersion>>): List<Int> {
    val own = rows[plugin].orEmpty()
    checkHistory(plugin, scripts, own)?.let { throw MigrationException(it.reason) }
    return own.filter { it.checksum == null }.map { it.version }.sorted()
}

/** Before [plugin] is declared ready: its rows must be exactly its jar's scripts. */
internal fun recheckReady(plugin: String, scripts: List<MigrationScript>, rows: Map<String, List<AppliedVersion>>) {
    val own = rows[plugin].orEmpty()
    checkHistory(plugin, scripts, own)?.let { throw MigrationException(it.reason) }
    if (own.size < scripts.size) throw MigrationException(
        "expected $plugin at version ${scripts.size}, but the database has version ${own.size}; restart the server")
}

/** Kahn's algorithm over [nodes]; among the ready steps the smallest (plugin id, then version) goes first. */
private fun topologicalOrder(nodes: Collection<StepKey>, dependencies: Map<StepKey, List<StepKey>>): List<StepKey> {
    val included = nodes.toSet()
    val waiting = HashMap<StepKey, Int>()
    val dependents = HashMap<StepKey, MutableList<StepKey>>()
    for (node in included) {
        val needs = dependencies[node].orEmpty().filter { it in included }
        waiting[node] = needs.size
        needs.forEach { dependents.getOrPut(it) { mutableListOf() }.add(node) }
    }
    val ready = PriorityQueue(included.filter { waiting[it] == 0 })
    val order = mutableListOf<StepKey>()
    while (ready.isNotEmpty()) {
        val next = ready.poll()
        order.add(next)
        dependents[next].orEmpty().forEach { if (waiting.merge(it, -1, Int::plus) == 0) ready.add(it) }
    }
    return order
}

/** Every step in [leftover] waits on another leftover step, so following those dependencies must close a cycle. */
private fun findCycle(start: StepKey, leftover: Set<StepKey>, dependencies: Map<StepKey, List<StepKey>>): List<StepKey> {
    val path = mutableListOf<StepKey>()
    val seen = HashMap<StepKey, Int>()
    var current = start
    while (current !in seen) {
        seen[current] = path.size
        path.add(current)
        current = dependencies.getValue(current).filter { it in leftover }.min()
    }
    return path.subList(seen.getValue(current), path.size) + current
}

private class UnionFind(nodes: Collection<StepKey>) {
    private val parent = HashMap<StepKey, StepKey>().apply { nodes.forEach { put(it, it) } }
    fun find(node: StepKey): StepKey {
        var root = node
        while (parent.getValue(root) != root) root = parent.getValue(root)
        parent[node] = root
        return root
    }
    fun union(a: StepKey, b: StepKey) {
        val (rootA, rootB) = find(a) to find(b)
        if (rootA != rootB) { if (rootA < rootB) parent[rootB] = rootA else parent[rootA] = rootB }
    }
}
