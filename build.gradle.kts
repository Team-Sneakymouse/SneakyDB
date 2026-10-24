import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.zip.ZipFile

plugins {
    kotlin("jvm") version "2.3.21"
    id("xyz.jpenilla.run-paper") version "3.0.2"
    `maven-publish`
}

group = "io.github.team-sneakymouse"

// Keep build outputs on a native FS when the checkout sits on FUSE/NTFS (e.g. /mnt/files).
layout.buildDirectory.set(
    file("${System.getProperty("user.home")}/.cache/${rootProject.name}-gradle-build"),
)

// Requires a git checkout with at least one commit (CI and local clones).
version =
    providers.exec {
        workingDir(rootDir)
        commandLine("git", "show", "-s", "--format=%ct:%h", "--abbrev=12", "HEAD")
    }.standardOutput.asText.map { commit ->
        val (timestamp, hash) = commit.trim().split(":", limit = 2)
        val date =
            DateTimeFormatter.ofPattern("yyyy.MM.dd").withZone(ZoneOffset.UTC)
                .format(Instant.ofEpochSecond(timestamp.toLong()))
        "$date-$hash"
    }.get()

repositories {
    mavenCentral()
    maven("https://repo.papermc.io/repository/maven-public/")
    // Internal Team-Sneakymouse / Lords of Minecraft artifacts:
    maven("https://maven.sneakyrp.com/releases")
}

val paperApi = "26.2.build.+"
val mockBukkitVersion = "4.117.0"

/**
 * On Windows, a bare `bash` resolves to WSL's System32\bash.exe before PATH is searched.
 * Use Git for Windows' bash (located via `git --exec-path`) when it exists.
 */
fun bashExecutable(): String {
    if (!System.getProperty("os.name").startsWith("Windows")) return "bash"
    val gitExecPath =
        providers.exec { commandLine("git", "--exec-path") }.standardOutput.asText.get().trim()
    val gitBash = file(gitExecPath).resolve("../../../bin/bash.exe").normalize()
    return if (gitBash.isFile) gitBash.absolutePath else "bash"
}

/**
 * Keys under paper-plugin.yml `dependencies.server` → required (default true).
 * Expects the template's 2-space indent shape; ignores comments and blank lines.
 */
fun parsePaperServerDependencies(yml: File): Map<String, Boolean> {
    if (!yml.isFile) return emptyMap()
    val lines = yml.readLines()
    fun indentOf(s: String) = s.takeWhile { it == ' ' }.length
    fun skippable(s: String) = s.isBlank() || s.trimStart().startsWith("#")

    var i = 0
    while (i < lines.size) {
        if (!skippable(lines[i]) && lines[i].trim() == "dependencies:") break
        i++
    }
    if (i >= lines.size) return emptyMap()
    val depsIndent = indentOf(lines[i])
    i++

    var serverIndent = -1
    while (i < lines.size) {
        val line = lines[i]
        if (skippable(line)) {
            i++
            continue
        }
        val ind = indentOf(line)
        if (ind <= depsIndent) return emptyMap()
        if (line.trim() == "server:") {
            serverIndent = ind
            i++
            break
        }
        i++
    }
    if (serverIndent < 0) return emptyMap()

    val result = linkedMapOf<String, Boolean>()
    var current: String? = null
    var required = true
    fun flush() {
        current?.let { result[it] = required }
        current = null
        required = true
    }

    val nameIndent = serverIndent + 2
    while (i < lines.size) {
        val line = lines[i]
        if (skippable(line)) {
            i++
            continue
        }
        val ind = indentOf(line)
        if (ind <= serverIndent) {
            flush()
            break
        }
        val trimmed = line.trim()
        if (ind == nameIndent && trimmed.endsWith(":")) {
            flush()
            current = trimmed.removeSuffix(":").trim()
            required = true
        } else if (current != null && trimmed.startsWith("required:")) {
            val value =
                trimmed
                    .substringAfter("required:")
                    .trim()
                    .removeSurrounding("\"")
                    .removeSurrounding("'")
            required = !value.equals("false", ignoreCase = true)
        }
        i++
    }
    flush()
    return result
}

/** Bukkit/Paper plugin id from a jar's paper-plugin.yml or plugin.yml `name:` field. */
fun pluginNameFromJar(jar: File): String? {
    if (!jar.isFile || !jar.name.endsWith(".jar", ignoreCase = true)) return null
    return try {
        ZipFile(jar).use { zip ->
            val entry = zip.getEntry("paper-plugin.yml") ?: zip.getEntry("plugin.yml") ?: return null
            zip.getInputStream(entry).bufferedReader().use { reader ->
                reader
                    .lineSequence()
                    .map { it.trim() }
                    .firstOrNull { it.startsWith("name:") }
                    ?.substringAfter("name:")
                    ?.trim()
                    ?.removeSurrounding("\"")
                    ?.removeSurrounding("'")
            }
        }
    } catch (_: Exception) {
        null
    }
}

/**
 * compileClasspath jars whose embedded plugin name matches a paper-plugin.yml server dependency.
 * (compileOnly itself is not resolvable in modern Gradle; compileClasspath includes those deps.)
 * Required deps with no match fail; optional deps log and skip.
 */
fun resolveRunServerPluginJars(): List<File> {
    val declared = parsePaperServerDependencies(file("src/main/resources/paper-plugin.yml"))
    if (declared.isEmpty()) return emptyList()

    val byName = linkedMapOf<String, File>()
    for (jar in configurations.compileClasspath.get().files) {
        val name = pluginNameFromJar(jar) ?: continue
        if (name in declared) {
            byName.putIfAbsent(name, jar)
        }
    }

    val missingRequired = declared.filter { (name, required) -> required && name !in byName }.keys
    if (missingRequired.isNotEmpty()) {
        throw GradleException(
            "runServer: required paper-plugin.yml server dependencies have no matching " +
                "compileOnly plugin jar: ${missingRequired.joinToString(", ")}. " +
                "Add compileOnly(\"group:artifact:version\") for each " +
                "(jar must contain paper-plugin.yml / plugin.yml with that name).",
        )
    }
    declared
        .filter { (name, required) -> !required && name !in byName }
        .keys
        .forEach { name ->
            logger.lifecycle(
                "runServer: optional server dependency {} has no matching compileOnly jar; skipping",
                name,
            )
        }
    return declared.keys.mapNotNull { byName[it] }
}

dependencies {
    compileOnly("io.papermc.paper:paper-api:$paperApi")
    implementation("org.jetbrains.kotlin:kotlin-stdlib:2.3.21")
    implementation("com.zaxxer:HikariCP:6.3.0")
    implementation("org.postgresql:postgresql:42.7.7")

    testImplementation("io.papermc.paper:paper-api:$paperApi")
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.4")
    testImplementation("org.mockbukkit.mockbukkit:mockbukkit-v26.2:$mockBukkitVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
    jvmToolchain(25)
}

java {
    withSourcesJar()
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_25)
    }
}

tasks {
    processResources {
        inputs.property("version", project.version.toString())
        filesMatching("paper-plugin.yml") {
            expand("version" to project.version.toString())
        }
    }

    jar {
        archiveBaseName.set(rootProject.name)
        archiveFileName.set("${rootProject.name}-${project.version}.jar")
        from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
        duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    }

    runServer {
        minecraftVersion("26.2")
        // Stage every paper-plugin.yml server dep whose compileOnly jar embeds that plugin name.
        pluginJars.from(provider { resolveRunServerPluginJars() })
        // Drop hand-copied / stale jars for those plugin names so only the resolved versions load.
        doFirst {
            val pluginsDir = runDirectory.get().asFile.resolve("plugins")
            if (!pluginsDir.isDirectory) return@doFirst
            val declaredNames =
                parsePaperServerDependencies(file("src/main/resources/paper-plugin.yml")).keys
            if (declaredNames.isEmpty()) return@doFirst
            pluginsDir
                .listFiles()
                ?.filter { it.isFile && it.name.endsWith(".jar", ignoreCase = true) }
                ?.filter { jar -> pluginNameFromJar(jar)?.let { it in declaredNames } == true }
                ?.forEach { stale ->
                    logger.lifecycle("runServer: removing stale {}", stale.name)
                    stale.delete()
                }
        }
    }

    // Focused dry resolve for runServer plugin staging (no server start).
    register("resolveRunServerPlugins") {
        group = "help"
        description = "Print paper-plugin.yml server dependency jars that runServer would stage."
        doLast {
            val jars = resolveRunServerPluginJars()
            if (jars.isEmpty()) {
                logger.lifecycle("runServer: no paper-plugin.yml server dependencies to stage")
                return@doLast
            }
            jars.forEach { jar ->
                logger.lifecycle("runServer: {} <- {}", pluginNameFromJar(jar), jar.name)
            }
        }
    }

    test {
        useJUnitPlatform()
    }

    val publishPreflight by registering(Exec::class) {
        group = "verification"
        description = "Catch sneakyrp publish footguns (gradlew mode, wrapper jar, workflow_dispatch)."
        workingDir = rootDir
        commandLine(bashExecutable(), "scripts/publish-preflight.sh", rootDir.absolutePath)
    }

    val templatePolicyPreflight by registering {
        group = "verification"
    description = "Catch EmptyPlugin-only agent rules copied into a renamed plugin."
        doLast {
            if (rootProject.name != "EmptyPlugin") {
                val instructions = rootProject.file("AGENTS.md")
                val marker = "<!-- EMPTYPLUGIN-ONLY LOCAL EXCEPTION: remove from repositories created from this template -->"
                if (instructions.isFile && marker in instructions.readText()) {
                    throw GradleException(
                        "AGENTS.md still contains the EmptyPlugin-only exception. Remove the template block; if needed, restate a separately approved exception with this repository's scope."
                    )
                }
            }
        }
    }

    check {
        dependsOn(publishPreflight, templatePolicyPreflight)
    }
}

publishing {
    publications {
        create<MavenPublication>("consumerApi") {
            artifactId = "sneakydb-api"
            artifact(tasks.register("apiJar", Jar::class) {
                dependsOn(tasks.named("classes"))
                archiveFileName.set("SneakyDB-api-${project.version}.jar")
                archiveClassifier.set("")
                from(sourceSets.main.get().output) {
                    include("com/sneakydb/SneakyDBApi.class")
                    include("com/sneakydb/SneakyDBProvider.class")
                }
            })
            pom {
                name.set("SneakyDB API")
                description.set("Java API for consumers of the SneakyDB Paper plugin.")
                url.set("https://github.com/Team-Sneakymouse/SneakyDB")
            }
        }
        create<MavenPublication>("maven") {
            artifactId = "sneakydb"
            from(components["java"])
            pom {
                name.set("SneakyDB")
                description.set("Shared asynchronous database API for Team-Sneakymouse Paper plugins.")
                url.set("https://github.com/Team-Sneakymouse/SneakyDB")
                properties.put("sneakyrp.deployment.type", "paper-plugin")
                licenses {
                    license {
                        name.set("GNU General Public License v3.0")
                        url.set("https://www.gnu.org/licenses/gpl-3.0.txt")
                    }
                }
                developers {
                    developer {
                        id.set("team-sneakymouse")
                        name.set("Team Sneakymouse")
                        url.set("https://github.com/Team-Sneakymouse")
                    }
                }
                scm {
                    url.set("https://github.com/Team-Sneakymouse/SneakyDB")
                    connection.set("scm:git:https://github.com/Team-Sneakymouse/SneakyDB.git")
                }
            }
        }
    }
    repositories {
        maven {
            name = "sneakyrp"
            url = uri("https://maven.sneakyrp.com/releases")
            credentials(PasswordCredentials::class)
            authentication {
                create<org.gradle.authentication.http.BasicAuthentication>("basic")
            }
        }
    }
}

tasks.withType<PublishToMavenRepository>().configureEach {
    dependsOn(tasks.check)
}
