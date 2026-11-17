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
/**
 * A comment line is a requires directive when its first word is `requires` or `requires-if-present` (any case) followed
 * by a colon, or followed by something shaped like `<id> >= <n>` (a forgotten colon). Other comments are prose.
 */
private val REQUIREMENT_LINE = Regex(
    "--\\s*(requires|requires-if-present)(\\s*:.*|\\s+[A-Za-z_][A-Za-z0-9_]*\\s*(>=|=>|<=|>|<|=).*)", RegexOption.IGNORE_CASE)
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
        findTransactionControl(text)?.let { (line, statement) ->
            throw MigrationException("$plugin: $name line $line: $statement controls the transaction, but SneakyDB already runs each " +
                "script in its own transaction together with its version row; remove BEGIN, COMMIT, ROLLBACK, SAVEPOINT and other transaction control")
        }
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
 * Reads `-- requires` directives from the leading comment block (blank lines and `--` comments before the first SQL).
 * A directive-shaped line (see [REQUIREMENT_LINE]) that doesn't parse, or one after the first SQL line, is an error,
 * so a typo can't drop a dependency silently.
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

private val TRANSACTION_CONTROL = setOf("begin", "commit", "end", "rollback", "abort", "savepoint", "release")
private val ROUTINE = setOf("function", "procedure")
private val DOLLAR_TAG = Regex("\\$([A-Za-z_\\u0080-\\uffff][A-Za-z0-9_\\u0080-\\uffff]*)?\\$")

/**
 * The first top-level statement of [sql] that controls the transaction (`BEGIN`, `START TRANSACTION`, `COMMIT`, `END`,
 * `ROLLBACK`, `ABORT`, `SAVEPOINT`, `RELEASE`, `PREPARE TRANSACTION`, `COMMIT PREPARED`, `ROLLBACK PREPARED`), as its
 * line and leading words, or null. Understands PostgreSQL's `--` and nested `/* */` comments, `'…'` and `E'…'`
 * strings, `"…"` identifiers and dollar-quoted bodies, so `BEGIN … END` inside a function body or `DO` block is not a
 * statement. Like psql, a `CREATE [OR REPLACE] FUNCTION|PROCEDURE` statement with a SQL-standard body
 * (`BEGIN ATOMIC … END`) keeps its semicolons until the matching `END`, counting `CASE … END` inside it. psql counts
 * any `BEGIN` there; only `BEGIN ATOMIC` counts here, so `RETURN begin` (a parameter) can't hide a later `COMMIT`.
 */
internal fun findTransactionControl(sql: String): Pair<Int, String>? = TransactionControlScanner(sql).find()

private class TransactionControlScanner(private val sql: String) {
    private var i = 0
    private var line = 1
    /** The current statement's first tokens: words in lower case, "" for anything else. */
    private val tokens = ArrayList<String>()
    private var statementLine = 1
    private var parens = 0
    private var atomicDepth = 0
    private var previousWord: String? = null

    fun find(): Pair<Int, String>? {
        while (i < sql.length) {
            val c = sql[i]
            val next = sql.getOrNull(i + 1)
            when {
                c == '\n' -> { line++; i++ }
                c.isWhitespace() -> i++
                c == '-' && next == '-' -> while (i < sql.length && sql[i] != '\n') i++
                c == '/' && next == '*' -> skipBlockComment()
                c == '\'' -> { token(""); skipQuoted('\'', backslash = false) }
                c == '"' -> { token(""); skipQuoted('"', backslash = false) }
                c == '$' -> { token(""); skipDollarQuoted() }
                c == ';' -> {
                    i++
                    if (atomicDepth == 0) statementEnd()?.let { return it }
                }
                c == '_' || c.isLetter() -> word()
                else -> {
                    token("")
                    if (c == '(') parens++ else if (c == ')' && parens > 0) parens--
                    i++
                }
            }
        }
        return statementEnd()
    }

    private fun token(value: String) {
        if (tokens.isEmpty()) statementLine = line
        if (tokens.size < 4) tokens.add(value)
    }

    private fun statementEnd(): Pair<Int, String>? {
        val first = tokens.getOrNull(0)
        val second = tokens.getOrNull(1)
        val found = when {
            first in TRANSACTION_CONTROL -> first!!.uppercase() + if (second == "prepared") " PREPARED" else ""
            (first == "start" || first == "prepare") && second == "transaction" -> "${first.uppercase()} TRANSACTION"
            else -> null
        }
        tokens.clear()
        parens = 0
        atomicDepth = 0
        previousWord = null
        return found?.let { statementLine to it }
    }

    private fun word() {
        val start = i
        while (i < sql.length && (sql[i] == '_' || sql[i] == '$' || sql[i].isLetterOrDigit())) i++
        val word = sql.substring(start, i).lowercase()
        if (word == "e" && sql.getOrNull(i) == '\'') {
            token("")
            skipQuoted('\'', backslash = true)
            return
        }
        token(word)
        val previous = previousWord
        previousWord = word
        if (parens != 0 || !isRoutine()) return
        when {
            word == "atomic" && previous == "begin" -> atomicDepth++
            word == "case" && atomicDepth > 0 -> atomicDepth++
            word == "end" && atomicDepth > 0 -> atomicDepth--
        }
    }

    private fun isRoutine(): Boolean = tokens.getOrNull(0) == "create" &&
        (tokens.getOrNull(1) in ROUTINE || (tokens.getOrNull(1) == "or" && tokens.getOrNull(2) == "replace" && tokens.getOrNull(3) in ROUTINE))

    /** Skips a run that ends at [quote], which is doubled to escape it; with [backslash] a backslash escapes too. */
    private fun skipQuoted(quote: Char, backslash: Boolean) {
        i++
        while (i < sql.length) {
            val c = sql[i++]
            if (c == '\n') line++
            if (backslash && c == '\\' && i < sql.length) {
                if (sql[i] == '\n') line++
                i++
            } else if (c == quote) {
                if (sql.getOrNull(i) == quote) i++ else return
            }
        }
    }

    private fun skipBlockComment() {
        var depth = 0
        while (i < sql.length) {
            when {
                sql.startsWith("/*", i) -> { depth++; i += 2 }
                sql.startsWith("*/", i) -> { depth--; i += 2; if (depth == 0) return }
                else -> { if (sql[i] == '\n') line++; i++ }
            }
        }
    }

    /** `$tag$ … $tag$`; a `$` that starts no tag (`$1`) is an ordinary token. */
    private fun skipDollarQuoted() {
        val tag = DOLLAR_TAG.matchAt(sql, i)?.value
        if (tag == null) { i++; return }
        val close = sql.indexOf(tag, i + tag.length)
        val end = if (close < 0) sql.length else close + tag.length
        for (index in i until end) if (sql[index] == '\n') line++
        i = end
    }
}
