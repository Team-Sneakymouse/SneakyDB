# SneakyDB agent instructions

This repository is a Paper plugin and consumer API for a shared database bridge. Follow the Team-Sneakymouse development policy through `.team-policy/pointer.json`; begin with `team-development-policy/README.md` and load relevant policy before nontrivial changes.

## Project facts

- Kotlin/Java 25, Paper API 26.2, Gradle wrapper.
- PostgreSQL is the only implemented adapter. Adapter implementation and JDBC/pool types stay behind the Java consumer API.
- Consumer boundary: `com.sneakydb.SneakyDBApi` and `SneakyDBProvider`; keep signatures to Java/JDK types for plugin classloader compatibility.
- The API and server plugin publish separately as `sneakydb-api` and `sneakydb`.
- Build outputs are written to `~/.cache/SneakyDB-gradle-build`.

## Architecture

- Keep consumer-facing interfaces Java-compatible and free of Kotlin, Paper, JDBC, HikariCP, and vendor driver types.
- Keep database connection and query work off the Paper main thread. Bound worker and pool sizes; close both with plugin lifecycle.
- Database-specific setup belongs in an adapter. Keep SQL supplied by consumers untouched; bind values with JDBC parameters.
- Treat config defaults, validation, error messages, and disable/reload lifecycle as part of the database boundary.

## Commands

- `./gradlew check` — tests and project verification.
- `./gradlew build` — build server jar and publications' artifacts.
- Builds derive versions from Git and require a checkout with at least one commit.

Do not commit credentials. Database passwords belong in the server config or secret injection, not source control.
