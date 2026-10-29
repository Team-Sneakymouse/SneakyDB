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

## Repo-specific rules

- Keep `SneakyDBApi`/`SneakyDBProvider` to JDK types only (`List`, `Map`, `CompletableFuture`). Add overloads rather than changing signatures, and grep consumer repos (SneakyJobBoard, SneakyPrototypeKit, LoMPhoneBridge) first.
- Keep the readiness contract (README "Readiness and lifecycle contract"): `whenReady()` returns a defensive copy per call (`ready.copy()`), succeeds only after a validated connection, and fails on init failure or shutdown. Update `ReadinessLoggingTest`/`DatabaseSessionTest` and the README when touching it.
- Failures surfaced through futures and logs must be credential-safe: wrap driver exceptions in a generic `IllegalStateException`; never log the password or a JDBC URL with credentials.
- `sneakydb-api` and `sneakydb` publish separately. Consumers use a version-pinned `compileOnly` dependency, so bump consumers deliberately after an API change.
- Run or add a disable/enable test when changing `DatabaseSession`; the worker executor and Hikari pool must shut down on disable.
