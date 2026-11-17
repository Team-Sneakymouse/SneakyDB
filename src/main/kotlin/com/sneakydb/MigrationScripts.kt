package com.sneakydb

import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.util.zip.ZipFile

// Finding and parsing a plugin's migration scripts: pure file handling, no database and no platform types.

/** Where scripts live inside a plugin's jar or class directory. */
internal const val MIGRATIONS_DIRECTORY = "db/migrations"

internal val PLUGIN_ID = Regex("[a-z][a-z0-9_]{0,62}")
private val SCRIPT_NAME = Regex("V([0-9]+)__([A-Za-z0-9][A-Za-z0-9_-]*)\\.sql")
private val REQUIREMENT_LINE = Regex("--\\s*requires.*", RegexOption.IGNORE_CASE)
private val REQUIREMENT = Regex("--\\s*(requires|requires-if-present):\\s*([a-z][a-z0-9_]{0,62})\\s*>=\\s*([0-9]{1,9})\\s*")

/** A failure whose message tells the plugin author or operator what to do. Never carries credentials. */
internal class MigrationException(message: String) : IllegalStateException(message)

/** `-- requires: <plugin> >= <version>`, or `-- requires-if-present:` when [ifPresent]. */
internal data class Requirement(val plugin: String, val version: Int, val ifPresent: Boolean) {
    override fun toString() = "${if (ifPresent) "requires-if-present" else "requires"}: $plugin >= $version"
}

/** One script: [text] is what runs (BOM removed, LF line endings) and what [checksum] covers. */
internal class MigrationScript(
    val version: Int,
    val fileName: String,
    val text: String,
    val checksum: String,
    val requirements: List<Requirement>,
)

/** Reads the scripts of [plugin] from the jar or class directory that [anchor] was loaded from, sorted by version. */
internal fun discoverMigrationScripts(plugin: String, anchor: Class<*>): List<MigrationScript> {
    val location = anchor.protectionDomain?.codeSource?.location
        ?: throw MigrationException("$plugin: can't tell which jar ${anchor.name} was loaded from; pass a class from the plugin's own jar to migrate()")
    val path = try { Path.of(location.toURI()) } catch (_: Exception) {
        throw MigrationException("$plugin: ${anchor.name} was not loaded from a local jar or directory; pass a class from the plugin's own jar to migrate()")
    }
    val files = if (Files.isDirectory(path)) readDirectoryScripts(plugin, migrationDirectories(path, anchor.classLoader))
    else readJarScripts(path)
    if (files.isEmpty()) {
        throw MigrationException("$plugin: no migration scripts found; add $MIGRATIONS_DIRECTORY/V001__<name>.sql to the jar that contains ${anchor.name}")
    }
    return parseMigrationScripts(plugin, files)
}

/**
 * The script directory for a class-directory anchor (tests): `<classes>/db/migrations`, or, because Gradle keeps
 * resources apart from classes, the one directory (not jar) on the anchor's class path that has `db/migrations`.
 */
private fun migrationDirectories(classes: Path, loader: ClassLoader?): Path? {
    val own = classes.resolve(MIGRATIONS_DIRECTORY)
    if (Files.isDirectory(own)) return own
    val candidates = (loader ?: return null).getResources(MIGRATIONS_DIRECTORY).toList()
        .filter { it.protocol == "file" }
        .map { Path.of(it.toURI()).toRealPath() }
        .distinct()
    if (candidates.size > 1) {
        throw MigrationException("several directories on the class path hold $MIGRATIONS_DIRECTORY (${candidates.joinToString()}); keep the scripts in one")
    }
    return candidates.singleOrNull()
}

/** File name → bytes of every file directly in [directory]; subdirectories are an error. */
private fun readDirectoryScripts(plugin: String, directory: Path?): Map<String, ByteArray> {
    if (directory == null) return emptyMap()
    return Files.list(directory).use { entries ->
        entries.toList().associate { file ->
            if (Files.isDirectory(file)) throw MigrationException("$plugin: $MIGRATIONS_DIRECTORY must not contain directories (${file.fileName})")
            file.fileName.toString() to Files.readAllBytes(file)
        }
    }
}

/** Entry name below `db/migrations/` → bytes; entries in subdirectories keep their slash so they fail the name check. */
internal fun readJarScripts(jar: Path): Map<String, ByteArray> = ZipFile(jar.toFile()).use { zip ->
    zip.entries().toList()
        .filter { !it.isDirectory && it.name.startsWith("$MIGRATIONS_DIRECTORY/") }
        .associate { entry -> entry.name.removePrefix("$MIGRATIONS_DIRECTORY/") to zip.getInputStream(entry).use { it.readBytes() } }
}

/** Validates names and numbering (1..n, no gaps or duplicates) and parses each script. */
internal fun parseMigrationScripts(plugin: String, files: Map<String, ByteArray>): List<MigrationScript> {
    if (files.isEmpty()) throw MigrationException("$plugin: no migration scripts found in $MIGRATIONS_DIRECTORY")
    val scripts = files.entries.map { (name, bytes) ->
        val match = SCRIPT_NAME.matchEntire(name) ?: throw MigrationException(
            "$plugin: $MIGRATIONS_DIRECTORY/$name is not a valid script name; use V<number>__<name>.sql, e.g. V001__init.sql")
        val version = match.groupValues[1].toIntOrNull()?.takeIf { it >= 1 }
            ?: throw MigrationException("$plugin: $name has an invalid number; numbers start at 1")
        val text = normalizeScript(decodeScript(plugin, name, bytes))
        MigrationScript(version, name, text, sha256(text), parseRequirements(plugin, name, version, text))
    }.sortedBy { it.version }
    scripts.groupBy { it.version }.values.firstOrNull { it.size > 1 }?.let { same ->
        throw MigrationException("$plugin: ${same.joinToString(" and ") { it.fileName }} have the same number; give each script its own")
    }
    scripts.forEachIndexed { index, script ->
        if (script.version != index + 1) {
            throw MigrationException("$plugin: script number ${index + 1} is missing before ${script.fileName}; numbers have no gaps")
        }
    }
    return scripts
}

private fun decodeScript(plugin: String, name: String, bytes: ByteArray): String = try {
    Charsets.UTF_8.newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
        .decode(java.nio.ByteBuffer.wrap(bytes)).toString()
} catch (_: CharacterCodingException) {
    throw MigrationException("$plugin: $name is not valid UTF-8; save it as UTF-8")
}

/** Removes a leading BOM and turns CRLF and lone CR into LF, so the checksum doesn't depend on the checkout's OS. */
internal fun normalizeScript(text: String): String = text.removePrefix("﻿").replace("\r\n", "\n").replace('\r', '\n')

internal fun sha256(text: String): String =
    MessageDigest.getInstance("SHA-256").digest(text.toByteArray(Charsets.UTF_8)).joinToString("") { "%02x".format(it) }

/**
 * Reads `-- requires` lines from the leading comment block (blank lines and `--` comments before the first SQL).
 * A `-- requires…` line that doesn't parse, or one after the first SQL line, is an error, so a typo can't drop a
 * dependency silently.
 */
internal fun parseRequirements(plugin: String, fileName: String, version: Int, text: String): List<Requirement> {
    val requirements = LinkedHashMap<Pair<String, Boolean>, Requirement>()
    var inHeader = true
    text.lineSequence().forEachIndexed { index, raw ->
        val line = raw.trim()
        if (inHeader && line.isNotEmpty() && !line.startsWith("--")) inHeader = false
        if (!REQUIREMENT_LINE.matches(line)) return@forEachIndexed
        val where = "$plugin: $fileName line ${index + 1}"
        if (!inHeader) throw MigrationException("$where: requires lines belong in the comment block before the first SQL statement")
        val match = REQUIREMENT.matchEntire(line) ?: throw MigrationException(
            "$where: can't read '$line'; write '-- requires: <plugin> >= <version>' or '-- requires-if-present: <plugin> >= <version>'")
        val requirement = Requirement(match.groupValues[2], match.groupValues[3].toInt(), match.groupValues[1] == "requires-if-present")
        if (requirement.plugin == plugin && requirement.version >= version) {
            throw MigrationException("$where: a script can't require its own plugin at version $version or later")
        }
        if (requirement.plugin == plugin) return@forEachIndexed // the previous script is required anyway
        val key = requirement.plugin to requirement.ifPresent
        val known = requirements[key]
        if (known == null || known.version < requirement.version) requirements[key] = requirement
    }
    return requirements.values.toList()
}
