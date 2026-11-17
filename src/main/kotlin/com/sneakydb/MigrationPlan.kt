package com.sneakydb

import java.util.PriorityQueue

// Planning a migration run: pure logic over registered scripts and the version table, no database access.

internal enum class MigrationMode { APPLY, VERIFY }

/** One row of `sneakydb.schema_migrations`; a null [checksum] was recorded by an operator. */
internal class AppliedVersion(val version: Int, val checksum: String?)

internal data class StepKey(val plugin: String, val version: Int) : Comparable<StepKey> {
    override fun compareTo(other: StepKey) = compareValuesBy(this, other, StepKey::plugin, StepKey::version)
}

/** A pending script with the requirements on other plugins to re-check under the lock before it runs. */
internal class PlannedStep(val plugin: String, val script: MigrationScript, val requirements: List<Requirement>) {
    val key get() = StepKey(plugin, script.version)
    override fun toString() = "$plugin:${script.fileName}"
}

/** Why a plugin's migrations failed; [script] names the script involved, when there is one. */
internal class PluginFailure(val script: String?, val reason: String)

internal class MigrationPlan(
    /** Plugins that fail before anything runs. */
    val failures: Map<String, PluginFailure>,
    /** Operator rows without a checksum to fill in, per plugin (apply mode only). */
    val checksumFills: Map<String, List<Pair<Int, String>>>,
    /** Every pending script of the plugins that didn't fail, in an order that satisfies all requirements. */
    val steps: List<PlannedStep>,
    /** For each step, the steps that require it directly. */
    val dependents: Map<StepKey, List<StepKey>>,
    /** The schema version each plugin that didn't fail reaches: its highest script. */
    val targetVersions: Map<String, Int>,
)

/**
 * Plans one run. [registered] maps each plugin of this batch to its scripts, or to the failure that discovering them
 * gave. [applied] holds the version table's rows for every plugin, registered or not. Ties in the order go by plugin
 * id, then version, so every server computes the same order.
 */
internal fun planMigrations(
    registered: Map<String, Result<List<MigrationScript>>>,
    applied: Map<String, List<AppliedVersion>>,
    mode: MigrationMode,
): MigrationPlan {
    val failures = sortedMapOf<String, PluginFailure>()
    val fills = sortedMapOf<String, List<Pair<Int, String>>>()
    val scripts = sortedMapOf<String, List<MigrationScript>>()
    val databaseVersion = { plugin: String -> applied[plugin].orEmpty().maxOfOrNull { it.version } ?: 0 }

    for ((plugin, discovered) in registered.toSortedMap()) {
        val own = discovered.getOrElse { failures[plugin] = PluginFailure(null, it.message ?: it.javaClass.simpleName); continue }
        val failure = checkAgainstDatabase(plugin, own, applied[plugin].orEmpty(), mode)
        if (failure != null) { failures[plugin] = failure; continue }
        scripts[plugin] = own
        if (mode == MigrationMode.APPLY) {
            applied[plugin].orEmpty().filter { it.checksum == null }.map { it.version to own[it.version - 1].checksum }
                .takeIf { it.isNotEmpty() }?.let { fills[plugin] = it }
        }
    }

    // Graph of pending scripts: each depends on its plugin's previous pending script and on the scripts it requires.
    val steps = sortedMapOf<StepKey, PlannedStep>()
    val dependencies = HashMap<StepKey, MutableList<StepKey>>()
    val unresolved = HashMap<StepKey, String>()
    for ((plugin, own) in scripts) {
        val current = databaseVersion(plugin)
        for (script in own.drop(current)) {
            val key = StepKey(plugin, script.version)
            val needs = mutableListOf<StepKey>()
            if (script.version - 1 > current) needs.add(StepKey(plugin, script.version - 1))
            val effective = mutableListOf<Requirement>()
            for (requirement in script.requirements) {
                val other = requirement.plugin
                val otherVersion = databaseVersion(other)
                if (otherVersion >= requirement.version) { effective.add(requirement); continue }
                val present = other in registered || applied[other].orEmpty().isNotEmpty()
                if (requirement.ifPresent && !present) continue
                effective.add(requirement)
                val otherScripts = scripts[other]
                when {
                    otherScripts != null && otherScripts.size >= requirement.version -> needs.add(StepKey(other, requirement.version))
                    unresolved.containsKey(key) -> {}
                    other in failures -> unresolved[key] = "${script.fileName} needs $requirement, but $other's migrations failed"
                    otherScripts != null -> unresolved[key] = "${script.fileName} needs $requirement, but the database has $other at " +
                        "version $otherVersion and its build on this server only has scripts up to ${otherScripts.size}; " +
                        "install the $other build that adds that script together with this one"
                    else -> unresolved[key] = "${script.fileName} needs $requirement, but the database has $other at version " +
                        "$otherVersion and $other has not registered migrations on this server; install that $other build first"
                }
            }
            steps[key] = PlannedStep(plugin, script, effective)
            dependencies[key] = needs
        }
    }

    // Requirements connect scripts into groups; a group with an unresolvable requirement or a cycle fails as a whole.
    val groups = UnionFind(steps.keys)
    dependencies.forEach { (step, needs) -> needs.forEach { groups.union(step, it) } }
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
        checksumFills = fills.filterKeys { it !in failures },
        steps = order.map { steps.getValue(it) },
        dependents = dependents,
        targetVersions = scripts.filterKeys { it !in failures }.mapValues { it.value.size },
    )
}

/** Strict version, checksum and verify-mode checks for one plugin against its rows in the version table. */
private fun checkAgainstDatabase(
    plugin: String,
    scripts: List<MigrationScript>,
    rows: List<AppliedVersion>,
    mode: MigrationMode,
): PluginFailure? {
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
    if (mode == MigrationMode.VERIFY && versions.size < scripts.size) {
        val pending = scripts.drop(versions.size)
        return PluginFailure(pending.first().fileName, "${pending.joinToString { it.fileName }} not applied, and this server runs " +
            "migrations in verify mode; start the server with migrations.mode: apply on this database first, then restart this one")
    }
    return null
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
