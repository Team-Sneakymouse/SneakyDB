# SneakyDB

SneakyDB is a Paper plugin that provides an asynchronous, database-neutral API for other plugins. The first adapter is PostgreSQL. Consumers use the same Java API regardless of adapter, while SQL syntax remains the responsibility of the query author and target database.

## Consumer API

Enter through `SneakyDBProvider.getApi()`. The published API exposes only Java and JDK types; HikariCP, JDBC connections, drivers, and adapter implementations stay inside SneakyDB.

```java
SneakyDBApi database = SneakyDBProvider.getApi();
database.whenReady()
    .thenCompose(ignored -> database.query(
        "SELECT id, name FROM players WHERE id = ?", java.util.List.of(playerId)))
    .thenAccept(rows -> {
        // No Bukkit world/player access here; schedule and fence it as shown below.
    });
```

### Readiness and lifecycle contract

`whenReady()` returns a new, defensive `CompletableFuture<Void>` for the current **enable generation**. Completing or cancelling your copy does not change another subscriber or the plugin's readiness. Subscribers registered before or after initialization observe the same terminal result.

- **Pending:** constructing a lazy connection pool is not readiness. A SneakyDB worker must acquire a real connection, successfully call `Connection.isValid(5)` (five-second validation timeout), and close that validation connection first.
- **Success:** `isAvailable()` is true after that initialization until shutdown. It is a snapshot of initialization/lifecycle state, **not** a health check; a later outage can still make a query fail. A successful readiness future remains successful after shutdown.
- **Failure:** readiness completes exceptionally with a credential-safe `IllegalStateException` identifying initialization failure and suggesting configuration/access checks. Raw driver exception messages/causes are intentionally not exposed through readiness. There is no SneakyDB initialization retry or polling loop. Fix the cause and disable/re-enable or restart; Hikari may perform its own internal connection attempts within configured limits.
- **Shutdown:** pending readiness and outstanding query/execute futures fail exceptionally. Queued database operations are discarded; running JDBC operations are interrupted best-effort and may already have affected the database. An exceptional future is not proof of rollback. Shutdown never waits for workers or pool closure on the server thread. A pool produced by late initialization is closed, never published.
- **Stale handles:** APIs belong to one enable generation. Old handles never attach to a new generation. Reacquire the provider API for a new enable. The provider is registered while initialization is pending and unavailable while disabled.
- **Operations:** `query`/`execute` fail fast via an exceptional future if called before readiness, after initialization failure, or after shutdown; they do not wait for initialization. Worker rejection (shutdown or overload) also returns an exceptional future, not a synchronous executor exception. Four workers and a queue of 256 bound submitted work.

Connection acquisition uses the configured `connection-timeout-ms` (clamped to 250–30000 ms). PostgreSQL connection/login timeouts round that value up to whole seconds, and validation uses the finite timeout above. These are driver/pool bounds, not an exact end-to-end startup deadline. SQL execution has no new statement timeout. Never `join()`, `get()`, sleep, or poll on the Minecraft thread. Non-async completion callbacks may run inline on the subscriber or completing thread; keep them short or select an appropriate executor. Consumer callback code must not block shutdown.

### Java startup: readiness → schema → reconciliation

Dependency load order makes the provider accessible, not database-ready. Sequence consumer schema setup and initial reconciliation explicitly; SneakyDB does not own consumer schemas. For example, in a consumer `JavaPlugin`:

```java
// Field; increment on the server thread in onEnable AND onDisable.
private volatile long startupGeneration;

@Override public void onEnable() {
    long generation = ++startupGeneration;
    SneakyDBApi database = SneakyDBProvider.getApi();
    database.whenReady()
        .thenCompose(ignored -> database.execute(
            "CREATE TABLE IF NOT EXISTS consumer_state (id VARCHAR(36) PRIMARY KEY)",
            java.util.List.of()))
        .thenCompose(ignored -> database.query(
            "SELECT id FROM consumer_state", java.util.List.of()))
        .whenComplete((rows, failure) -> {
            if (generation != startupGeneration) return;
            try {
                getServer().getScheduler().runTask(this, () -> {
                    if (!isEnabled() || generation != startupGeneration) return;
                    if (failure != null) {
                        getLogger().warning("Database startup failed; reconciliation skipped");
                        return;
                    }
                    if (!database.isAvailable()) return;
                    // Reconcile rows with Bukkit state here, on the server thread.
                });
            } catch (org.bukkit.plugin.IllegalPluginAccessException disabled) {
                // Consumer was disabled before the async completion could schedule.
            }
        });
}

@Override public void onDisable() {
    ++startupGeneration; // Fence scheduled/in-flight work from the previous enable.
}
```

If reconciliation itself needs more database writes, compose their futures too; do not assume submission means completion. The fence protects Bukkit effects, not database writes already in flight. Consumers needing transactional cancellation must implement their own database-level protocol.

### Compatibility

Existing `query(String, List)`, `execute(String, List)`, parameter binding, and row shapes are unchanged. `whenReady()` adds a Java/JDK-only method; rebuild custom implementations of `SneakyDBApi` and run consumers requiring it with a matching/newer server API. Older consumers can still use the original methods, but pre-readiness calls now fail deterministically rather than racing initialization. Do not shade the API or depend on internal session/adapter classes.

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
