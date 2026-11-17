package com.sneakydb

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.Path
import java.util.jar.JarEntry
import java.util.jar.JarOutputStream

/** Loaded from a temporary directory or jar so that its code source is that location. */
class MigrationAnchorFixture

class MigrationScriptsTest {
    companion object {
        /** Parses [files] (name → text) as [plugin]'s scripts. */
        internal fun scripts(plugin: String, vararg files: Pair<String, String>): List<MigrationScript> =
            parseMigrationScripts(plugin, files.associate { (name, text) -> name to text.toByteArray() })

        private val anchorBytes: ByteArray =
            MigrationAnchorFixture::class.java.getResourceAsStream("MigrationAnchorFixture.class")!!.readAllBytes()
        private const val ANCHOR = "com/sneakydb/MigrationAnchorFixture.class"
    }

    private fun failure(plugin: String = "plots", vararg files: Pair<String, String>): String =
        assertThrows(MigrationException::class.java) { scripts(plugin, *files) }.message!!

    private fun write(root: Path, relative: String, bytes: ByteArray) {
        val file = root.resolve(relative)
        Files.createDirectories(file.parent)
        Files.write(file, bytes)
    }

    private val loaders = mutableListOf<URLClassLoader>()

    /** Windows can't delete a jar an open class loader holds, so close them before the temporary directory goes. */
    @org.junit.jupiter.api.AfterEach fun closeLoaders() = loaders.forEach { it.close() }

    private fun anchorFrom(vararg roots: Path): Class<*> =
        URLClassLoader(roots.map { it.toUri().toURL() }.toTypedArray(), null).also { loaders.add(it) }
            .loadClass("com.sneakydb.MigrationAnchorFixture")

    @Test fun `scripts are discovered in a class directory`(@TempDir root: Path) {
        write(root, ANCHOR, anchorBytes)
        write(root, "db/migrations/V002__second.sql", "select 2;".toByteArray())
        write(root, "db/migrations/V001__init.sql", "select 1;".toByteArray())
        val found = discoverMigrationScripts("plots", anchorFrom(root))
        assertEquals(listOf(1 to "V001__init.sql", 2 to "V002__second.sql"), found.map { it.version to it.fileName })
        assertEquals("select 2;", found[1].text)
    }

    @Test fun `scripts are discovered in the jar the anchor was loaded from`(@TempDir root: Path) {
        val jar = root.resolve("plots.jar")
        JarOutputStream(Files.newOutputStream(jar)).use { out ->
            for ((name, bytes) in listOf(ANCHOR to anchorBytes, "db/migrations/V1__init.sql" to "select 1;".toByteArray(), "other/V9__x.sql" to byteArrayOf())) {
                out.putNextEntry(JarEntry(name)); out.write(bytes); out.closeEntry()
            }
        }
        assertEquals(listOf("V1__init.sql"), discoverMigrationScripts("plots", anchorFrom(jar)).map { it.fileName })
    }

    @Test fun `a subdirectory in the jar's script directory is a naming error`(@TempDir root: Path) {
        val jar = root.resolve("plots.jar")
        JarOutputStream(Files.newOutputStream(jar)).use { out ->
            for ((name, bytes) in listOf(ANCHOR to anchorBytes, "db/migrations/V1__init.sql" to byteArrayOf(), "db/migrations/old/V2__x.sql" to byteArrayOf())) {
                out.putNextEntry(JarEntry(name)); out.write(bytes); out.closeEntry()
            }
        }
        val error = assertThrows(MigrationException::class.java) { discoverMigrationScripts("plots", anchorFrom(jar)) }
        assertTrue(error.message!!.contains("old/V2__x.sql is not a valid script name"), error.message)
    }

    @Test fun `class directories find scripts in the separate resource directory Gradle uses`(@TempDir root: Path) {
        val classes = root.resolve("classes").also { write(it, ANCHOR, anchorBytes) }
        val resources = root.resolve("resources").also { write(it, "db/migrations/V001__init.sql", "select 1;".toByteArray()) }
        assertEquals(listOf("V001__init.sql"), discoverMigrationScripts("plots", anchorFrom(classes, resources)).map { it.fileName })

        val other = root.resolve("other").also { write(it, "db/migrations/V001__init.sql", byteArrayOf()) }
        val ambiguous = assertThrows(MigrationException::class.java) { discoverMigrationScripts("plots", anchorFrom(classes, resources, other)) }
        assertTrue(ambiguous.message!!.contains("several directories"), ambiguous.message)
    }

    @Test fun `a plugin without scripts fails with what to add`(@TempDir root: Path) {
        write(root, ANCHOR, anchorBytes)
        val error = assertThrows(MigrationException::class.java) { discoverMigrationScripts("plots", anchorFrom(root)) }
        assertEquals("plots: no migration scripts found; add db/migrations/V001__<name>.sql to the jar that contains com.sneakydb.MigrationAnchorFixture",
            error.message)
    }

    @Test fun `names must be V number double underscore name dot sql`() {
        for (bad in listOf("V1_init.sql", "v1__init.sql", "V1__.sql", "V1__init.SQL", "Vx__init.sql", "README.md", "V1__in it.sql")) {
            assertTrue(failure("plots", bad to "").contains("$bad is not a valid script name"), bad)
        }
        assertTrue(failure("plots", "V0__init.sql" to "").contains("numbers start at 1"))
        assertEquals(listOf(1, 2), scripts("plots", "V1__a-b_c.sql" to "", "V0002__x.sql" to "").map { it.version })
    }

    @Test fun `numbers start at 1 without gaps or duplicates`() {
        assertTrue(failure("plots", "V001__a.sql" to "", "V01__b.sql" to "").contains("have the same number"))
        assertTrue(failure("plots", "V001__a.sql" to "", "V003__c.sql" to "").contains("script number 2 is missing before V003__c.sql"))
        assertTrue(failure("plots", "V002__b.sql" to "").contains("script number 1 is missing"))
        assertTrue(assertThrows(MigrationException::class.java) { parseMigrationScripts("plots", emptyMap()) }.message!!.contains("no migration scripts"))
    }

    @Test fun `checksums ignore line endings and a leading BOM but not content`() {
        val lf = scripts("plots", "V1__a.sql" to "create table a (id int);\nselect 1;\n").single()
        val crlf = scripts("plots", "V1__a.sql" to "create table a (id int);\r\nselect 1;\r\n").single()
        val bom = scripts("plots", "V1__a.sql" to "﻿create table a (id int);\rselect 1;\r").single()
        assertEquals(lf.checksum, crlf.checksum)
        assertEquals(lf.checksum, bom.checksum)
        assertEquals(lf.text, crlf.text)
        assertEquals(64, lf.checksum.length)
        assertNotEquals(lf.checksum, scripts("plots", "V1__a.sql" to "create table a (id bigint);\nselect 1;\n").single().checksum)
        assertEquals("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", sha256(""))
    }

    @Test fun `scripts must be UTF-8`() {
        val error = assertThrows(MigrationException::class.java) {
            parseMigrationScripts("plots", mapOf("V1__a.sql" to byteArrayOf(0xC3.toByte(), 0x28)))
        }
        assertTrue(error.message!!.contains("not valid UTF-8"))
    }

    @Test fun `requires lines in the leading comment block are parsed`() {
        val script = scripts("plots", "V1__a.sql" to "", "V2__b.sql" to """
            -- Moves plots to characters.
            --requires: sneakycharacters >= 3

            --   requires-if-present:   sneakyusermeta>=5
            -- requires: sneakycharacters >= 2
            -- requires: plots >= 1
            create table x (id int);
            -- a later comment that mentions requirements is fine
        """.trimIndent())[1]
        assertEquals(listOf(Requirement("sneakycharacters", 3, false), Requirement("sneakyusermeta", 5, true)), script.requirements)
    }

    @Test fun `comments that merely start with the word requires are prose`() {
        val script = scripts("plots", "V1__a.sql" to "", "V2__b.sql" to """
            -- Requirements: the plots table from V1.
            -- requires the plots table from V1
            -- Requires a restart afterwards.
            -- require the plots table too
            create table x (id int);
            -- requires the index above
        """.trimIndent())[1]
        assertEquals(emptyList<Requirement>(), script.requirements)
    }

    @Test fun `a malformed or misplaced requires line fails the plugin`() {
        val malformed = listOf(
            "-- requires sneakycharacters >= 3", "-- requires: sneakycharacters > 3", "-- Requires: sneakycharacters >= 3",
            "-- requires: SneakyCharacters >= 3", "-- requires: sneakycharacters >= x", "-- requires-if-present sneakyusermeta >= 1",
            "-- requires: sneakycharacters >= 3, sneakyusermeta >= 1", "-- REQUIRES sneakycharacters >= 3", "-- requires sneakycharacters > 3",
            "-- requires: the plots table", "-- require: core >= 1", "-- require core >= 1", "-- require-if-present: core >= 1",
        )
        for (line in malformed) {
            val message = failure("plots", "V1__a.sql" to "$line\nselect 1;")
            assertTrue(message.startsWith("plots: V1__a.sql line 1: can't read"), "$line → $message")
        }
        val late = failure("plots", "V1__a.sql" to "-- header\nselect 1;\n-- requires: sneakycharacters >= 3\n")
        assertTrue(late.contains("line 3: requires lines belong in the comment block before the first SQL statement"), late)
        val afterBlockComment = failure("plots", "V1__a.sql" to "/* header */\n-- requires: sneakycharacters >= 3\n")
        assertTrue(afterBlockComment.contains("before the first SQL statement"), afterBlockComment)
        val self = failure("plots", "V1__a.sql" to "", "V2__b.sql" to "-- requires: plots >= 2\nselect 1;")
        assertTrue(self.contains("can't require its own plugin at version 2 or later"), self)
    }

    @Test fun `scripts that control the transaction at the top level fail the plugin`() {
        val cases = listOf(
            "begin;\ncreate table a (id int);\ncommit;" to "line 1: BEGIN",
            "create table a (id int);\nCOMMIT;\ninsert into missing values (1);" to "line 2: COMMIT",
            "create table a (id int);\n  rollback ;\ncreate table b (id int);" to "line 2: ROLLBACK",
            "-- header\nstart transaction isolation level serializable;\nselect 1;" to "line 2: START TRANSACTION",
            "select 1; end;" to "line 1: END",
            "select 1;\nabort" to "line 2: ABORT",
            "savepoint s;\nselect 1;" to "line 1: SAVEPOINT",
            "select 1;\n/* a comment */ release savepoint s;" to "line 2: RELEASE",
            "prepare transaction 'x';" to "line 1: PREPARE TRANSACTION",
            "commit prepared 'x';" to "line 1: COMMIT PREPARED",
            "rollback prepared 'x';" to "line 1: ROLLBACK PREPARED",
            "create function d(begin int) returns int language sql return begin;\ncommit;" to "line 2: COMMIT",
            "select 'it''s';\nselect E'\\'; commit; \\'';\nselect \"a;b\";\nselect \$q\$ ; \$q\$;\n\ncommit;" to "line 6: COMMIT",
            // begin and atomic with an operator between them are two parameters, not a BEGIN ATOMIC body.
            "create function f(begin int, atomic int) returns int language sql return begin + atomic;\ncommit;" to "line 2: COMMIT",
            "create function f(begin int, atomic int) returns int language sql return begin /* c */ + atomic;\ncommit;" to "line 2: COMMIT",
            // PostgreSQL reads a character at or above U+0080 followed by a dollar tag as one identifier, not a dollar quote.
            "select 1 as €\$q\$;\ncommit;\nselect 2 as €\$q\$;" to "line 2: COMMIT",
            "select 1 as e\u0301\$q\$;\ncommit;\nselect 2 as e\u0301\$q\$;" to "line 2: COMMIT",
            "select 1 as a\u00a0\$q\$;\ncommit;\nselect 2 as a\u00a0\$q\$;" to "line 2: COMMIT",
            "select 1 as \u00e9\$q\$;\ncommit;\nselect 2 as \u00e9\$q\$;" to "line 2: COMMIT",
            // The body ends at the END that starts a statement inside it; the next END is top-level again.
            "create function f() returns int language sql begin atomic select 1; end;\nend;" to "line 2: END",
            "create function f() returns int language sql begin /* c */ atomic select 1 as end; end;\ncommit;" to "line 2: COMMIT",
        )
        for ((text, where) in cases) {
            val message = failure("plots", "V1__a.sql" to text)
            assertTrue(message.startsWith("plots: V1__a.sql $where controls the transaction"), "$text → $message")
        }
    }

    @Test fun `transaction words inside bodies, strings, identifiers and comments are not transaction control`() {
        val text = """
            -- commit; rollback; begin;
            /* outer /* nested; commit; */ still a comment; rollback; */
            create function a() returns int language plpgsql as $$
            begin
                return 1;
            end;
            $$;
            create or replace procedure b() language plpgsql as ${'$'}body$ begin commit; end ${'$'}body$;
            do $$ begin perform 1; end $$;
            do 'begin perform 1; end';
            select 'commit; rollback;', E'it\'s; commit;', "commit;", U&'x; commit;' as end;
            select ${'$'}1;
            create table x (begin int, "end" int, commit${'$'}x int);
            create function c() returns int language sql begin atomic select case when true then 1 end; select 2; end;
            create function d(begin int) returns int language sql return begin;
        """.trimIndent()
        assertEquals(1, scripts("plots", "V1__a.sql" to text).size)
    }

    @Test fun `legitimate scripts around BEGIN ATOMIC bodies, END columns and non-ASCII identifiers pass`() {
        val legitimate = listOf(
            // A column alias named end inside a BEGIN ATOMIC body is a label, not the body's END.
            "CREATE FUNCTION example() RETURNS integer LANGUAGE SQL\nBEGIN ATOMIC\n    SELECT 1 AS end;\nEND;",
            "create function e() returns int language sql begin atomic select 1 end; end;\nselect 2;",
            "create function e() returns int language sql begin atomic select \"end\" from t; end;",
            "create function e() returns int language sql begin atomic select case when true then 1 else 2 end as end; select 3; end;",
            "create function e() returns int language sql begin atomic select case when true then case when false then 1 end end end; end;",
            "create function e() returns void language sql begin atomic end;",
            "create function e() returns int language sql begin\n-- c\natomic select 1; end;\nselect 2;",
            "create function e(begin int, atomic int) returns int language sql return begin + atomic;",
            "create table t (\"end\" int, \"commit\" int);\nselect \"end\", case when true then 1 end as end from t;\nselect case when \"end\" > 0 then 'commit' end from t;",
            "create function p() returns int language plpgsql as \$\$ begin if true then return 1; end if; end \$\$;",
            "comment on function p() is 'commit; rollback;';\ncomment on table t is \$c\$ end; \$c\$;",
            "select 'rollback;', E'it\\'s; rollback;', \$\$ rollback; \$\$;",
            "prepare q as select 1;\nexecute q;\ndeallocate q;",
            "create table café (prix€ int, été int);\nselect prix€ as €\$q from café;",
            "select \$x1\$ commit; \$x1\$;",
        )
        for (text in legitimate) {
            assertNull(findTransactionControl(text), text)
            assertEquals(1, scripts("plots", "V1__a.sql" to text).size, text)
        }
    }
}
