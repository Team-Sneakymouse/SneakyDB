# SneakyDB

SneakyDB is a Paper plugin that provides an asynchronous, database-neutral API for other plugins. The first adapter is PostgreSQL. Consumers use the same Java API regardless of adapter, while SQL syntax remains the responsibility of the query author and target database.

## Consumer API

Enter through `SneakyDBProvider.getApi()`. The published API exposes only Java and JDK types; HikariCP, JDBC connections, drivers, and adapter implementations stay inside SneakyDB.

```kotlin
val database = SneakyDBProvider.getApi()
database.query(
    "SELECT id, name FROM players WHERE id = ?",
    listOf(playerId),
).thenAccept { rows ->
    // Runs asynchronously. Switch to the Bukkit thread before using Bukkit APIs.
}
```

`query` returns rows as maps keyed by column label. `execute` is for insert/update/delete and returns the affected row count. Both use JDBC `?` parameters and return `CompletableFuture`; never block the server thread waiting for a result. Queries use the target database's SQL dialect. Keep table and column identifiers trusted; bind values through `parameters` rather than concatenating them into SQL.

Consumers declare SneakyDB as a required joined classpath dependency:

```yaml
dependencies:
  server:
    SneakyDB:
      load: BEFORE
      required: true
      join-classpath: true
```

Compile against the API artifact, not the shaded server plugin:

```kotlin
repositories { maven("https://maven.sneakyrp.com/releases") }
dependencies { compileOnly("io.github.team-sneakymouse:sneakydb-api:<version>") }
```

Install the matching `SneakyDB-<version>.jar` on the server. Do not shade API classes into consumer plugins.

## PostgreSQL configuration

The default `plugins/SneakyDB/config.yml` contains:

```yaml
database:
  adapter: postgresql
  host: localhost
  port: 5432
  name: minecraft
  username: minecraft
  password: change-me
  maximum-pool-size: 10
  connection-timeout-ms: 5000
```

Set credentials for your server before enabling the plugin. Database connection and query work runs on SneakyDB's worker threads. The PostgreSQL driver and connection pool are bundled in the server jar.

## Adapter design

Adapters are selected and constructed inside SneakyDB. Each adapter supplies a pooled JDBC data source; consumers continue to call `query` and `execute` without depending on that adapter. Adding another database means adding its driver and adapter implementation, and registering its adapter identifier. SQL stays portable only to the extent that the caller writes portable SQL.

## Build and publish

The plugin uses the EmptyPlugin Gradle wrapper and Team-Sneakymouse versioning. Builds require a git checkout with at least one commit. `./gradlew build` creates the plugin jar; Maven publications are `sneakydb` (server plugin) and `sneakydb-api` (consumer compile-time API). Publish to `https://maven.sneakyrp.com/releases` through the repository's configured GitHub Actions workflow.
