# SneakyDB

SneakyDB is a Paper plugin, and from the same JAR a Velocity plugin (see [Velocity](#velocity)), that provides an asynchronous, database-neutral API for other plugins. The first adapter is PostgreSQL. Consumers use the same Java API regardless of adapter, while SQL syntax remains the responsibility of the query author and target database.

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
- **Shutdown:** pending readiness and outstanding query/execute/transaction futures fail exceptionally. Queued database operations are discarded; running JDBC operations are interrupted best-effort and may already have affected the database. For `execute`, an exceptional future is not proof of rollback. A transaction stays all-or-nothing: shutdown makes its next handle call throw and SneakyDB rolls it back instead of committing, or the pool closure aborts its connection and PostgreSQL discards the uncommitted work. Only a commit already in flight when shutdown arrives can still succeed after its future failed. Shutdown never waits for workers, transactions or pool closure on the server thread. A pool produced by late initialization is closed, never published.
- **Stale handles:** APIs belong to one enable generation. Old handles never attach to a new generation. Reacquire the provider API for a new enable. The provider is registered while initialization is pending and unavailable while disabled.
- **Operations:** `query`/`execute`/`transaction` fail fast via an exceptional future if called before readiness, after initialization failure, or after shutdown; they do not wait for initialization. Worker rejection (shutdown or overload) also returns an exceptional future, not a synchronous executor exception. Four workers and a queue of 256 bound submitted work.

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

Existing `query(String, List)`, `execute(String, List)`, parameter binding, and row shapes are unchanged. `transaction(Function)` and the `SneakyDBTransaction` interface are additions (see [Transactions](#transactions)). `transaction` has a default body that throws `UnsupportedOperationException`, so existing custom implementations and test doubles of `SneakyDBApi` keep compiling; override it in a test double to exercise transaction code. A consumer that calls `transaction` needs a server running this SneakyDB version or newer; on an older server the call fails with `NoSuchMethodError`. `whenReady()` adds a Java/JDK-only method; rebuild custom implementations of `SneakyDBApi` and run consumers requiring it with a matching/newer server API. Older consumers can still use the original methods, but pre-readiness calls now fail deterministically rather than racing initialization. Do not shade the API or depend on internal session/adapter classes.

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
repositories { maven("https://maven.sneakyrp.com/sneakymouse") }
dependencies { compileOnly("io.github.team-sneakymouse:sneakydb-api:<version>") }
```

Install the matching `SneakyDB-<version>.jar` on the server. Do not shade API classes into consumer plugins.

## Transactions

`transaction(work)` runs several statements atomically. `work` receives a `SneakyDBTransaction` handle whose `query` and `execute` block and run in the transaction; its return value becomes the future's value.

```java
database.transaction(tx -> {
    int debited = tx.execute(
        "UPDATE wallets SET balance = balance - ? WHERE id = ? AND balance >= ?", java.util.List.of(amount, from, amount));
    if (debited == 0) throw new IllegalStateException("insufficient funds"); // rolls back
    tx.execute("UPDATE wallets SET balance = balance + ? WHERE id = ?", java.util.List.of(amount, to));
    return tx.query("SELECT balance FROM wallets WHERE id = ?", java.util.List.of(from)).get(0).get("balance");
}).whenComplete((balance, failure) -> {
    // Runs on a SneakyDB thread; reschedule before touching Bukkit state.
});
```

```kotlin
database.transaction { tx ->
    tx.execute("DELETE FROM claims WHERE player = ?", listOf(player))
    tx.execute("INSERT INTO claims (player, plot) VALUES (?, ?)", listOf(player, plot))
}.whenComplete { inserted, failure -> /* runs on a SneakyDB thread */ }
```

- `work` runs on a SneakyDB worker that holds one pooled connection with autocommit off. Keep it short and database-only: until it returns it occupies one of the four workers, one pool connection and the transaction's locks. No Bukkit access, no waiting on other threads.
- When `work` returns, SneakyDB commits and completes the future with the returned value (which may be null). When `work` throws, SneakyDB rolls back and the future completes exceptionally with that exception. A failed commit fails the future with the driver's `SQLException`. The future never completes normally unless the commit succeeded.
- A failed statement throws its `SQLException` out of `query`/`execute`. The interface doesn't declare it, so Java code inside `work` can only catch it as `Exception`. PostgreSQL aborts the whole transaction after a failed statement, so let it propagate; to retry (for example on SQL state `40001` or `40P01`), call `transaction` again with the same work.
- The connection goes back to the pool in autocommit mode with no transaction open. If the rollback itself fails, SneakyDB discards the connection instead of returning it.
- The handle is valid only inside `work`, on the worker thread. Using it after `work` returns, or from another thread, throws `IllegalStateException`. Do the database work synchronously: returning a future from `work` does not keep the transaction open.
- **No SneakyDB calls inside `work`:** `database.query`, `execute` and `transaction` called from inside `work` fail with `IllegalStateException` through their future, so a `join()` rethrows it and the transaction rolls back. Such a call would run on another connection outside the transaction, and waiting for it could starve the four workers or deadlock on the transaction's own locks. SneakyDB cannot detect the same call made from another thread that `work` waits for, so don't do that either.
- Admission matches `query`/`execute`: the future fails before readiness, after initialization failure, after shutdown, and when the worker queue is full. It never waits for readiness. For shutdown during a transaction, see [the lifecycle contract](#readiness-and-lifecycle-contract).
- Completing or cancelling the returned future before `work` returns rolls the transaction back: the next handle call throws `IllegalStateException`, and SneakyDB rolls back instead of committing. It does not interrupt `work`.
- Statements use the database's default isolation level (`READ COMMITTED` on PostgreSQL).

## Notifications (LISTEN / NOTIFY)

`listen(channel, onNotification, onConnected)` subscribes to PostgreSQL notifications; send them with `NOTIFY channel, 'payload'` or `pg_notify('channel', 'payload')`, for example from a trigger. Close the returned `AutoCloseable` to unsubscribe.

```kotlin
// Subscribe once queries work: onConnected re-reads with query(), which fails before readiness.
database.whenReady().thenRun {
    subscription = database.listen("discord_push_tracking", { payload -> refresh(payload) }) { refreshAll() }
}
```

- Channel names are lower-case identifiers (`[a-z_][a-z0-9_]{0,62}`); anything else throws `IllegalArgumentException`.
- Callbacks run on the single `SneakyDB-Notifications` thread, one at a time. Keep them short, and schedule onto the server thread before touching Bukkit state. A callback that throws is logged with its channel and the error's class, never its message, and does not affect other subscriptions. Connection warnings likewise name the error's class and SQL state only.
- Notifications are not queued while the connection is down. `onConnected` runs every time the subscription starts listening: first, and again after each reconnect. It always runs before that connection delivers the subscription any notification. Re-read the state you track there instead of relying on every notification arriving.
- The first subscription opens one dedicated, unpooled connection (one more beyond `database.maximum-pool-size` and the health check). Its reads time out after 15 seconds. It sends a keep-alive check every 30 seconds and, after a failure, reconnects with backoff from 1 to 30 seconds, logging one warning per outage.
- Closing the last subscription stops listening on its channel but keeps the connection and thread until shutdown; subscriptions are meant to last for a plugin's enable.
- `listen` may be called before readiness, but `onConnected` can then run before queries work. It throws `IllegalStateException` after initialization failure or shutdown; shutdown ends every subscription.

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

## Database-outage maintenance fallback

Startup initialization failure, or the first unsuccessful background connectivity check after successful initialization, requests **`cmi maintenance true <message>`** on the server thread. This is an explicit enable, never the argument-free toggle. The command contract comes from [CMI's official command reference](https://www.zrips.net/cmi/commands/) (`maintenance (true/false) (message)`, bypass permission `cmi.command.maintenance.bypass`). CMI owns player admission, bypass rules, message formatting, and any effects on already-connected players.

```yaml
maintenance:
  message: 'Database unavailable. Please try again later.'
  health-check-interval-seconds: 30
```

The message applies to both startup and runtime outages. Blank, control-character-containing, or over-512-character messages use the shown default. The interval is clamped to 5–300 seconds and is a delay **after** a completed check; the first check starts immediately after readiness. Config is read on enable; restart or disable/re-enable to apply edits (no hot reload command).

Health checks use a dedicated single-thread scheduler and a separate one-connection PostgreSQL pool, not the consumer worker queue or consumer pool. Allow **one additional database connection** beyond `database.maximum-pool-size`. Acquisition, driver connect/login/socket waits, and `Connection.isValid(5)` validation have five-second bounds; these are individual JDBC/driver bounds, not a hard end-to-end deadline. Checks do not overlap or accumulate, and shutdown fences queued main-thread effects and closes health resources off-thread. Consumer SQL syntax/constraint errors, worker rejection, and consumer pool contention alone do not activate maintenance.

**No automatic reopening:** only one maintenance command is attempted per enable generation, including across recovery and subsequent outages. Neither recovery nor disable/re-enable sends `false` or a toggle, so existing administrator maintenance stays enabled; an outage may replace its player-facing message. Operators must verify the database and manually reopen using CMI after recovery. A failed initial startup remains terminal: the health loop does not retry initialization. Fix configuration/access and restart or disable/re-enable first. The Java consumer API, historical `whenReady()`, and lifecycle-only `isAvailable()` semantics are unchanged.

**CMI limitation:** CMI is an optional, ordered server dependency, accessed by command without a joined classpath. If CMI is missing/disabled, or dispatch fails, SneakyDB emits a credential-safe severe diagnostic and does not claim the server is protected. There is **no independent login/kick guard**, no automatic command retry, and no proof of admission policy from Bukkit's command-dispatch boolean. Installing/enabling CMI later requires a new SneakyDB enable generation or manual maintenance activation. Ensure CMI maintenance is available and correctly configured before relying on this fallback; close access manually if it is unavailable. This integration is unit/MockBukkit verified, not a live CMI-server test.

## Velocity

The same `SneakyDB-<version>.jar` is also a Velocity plugin (`velocity-plugin.json`, id `sneakydb`). Put it in the proxy's `plugins/` folder. Paper ignores `velocity-plugin.json` and Velocity ignores `paper-plugin.yml`; the Velocity entry point (`com.sneakydb.velocity.SneakyDBVelocity`) loads no Bukkit classes.

### Proxy configuration

The first proxy start writes `plugins/sneakydb/config.yml`:

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

The keys, defaults and bounds are the same as the Paper `database:` section, so copy the values from a server's `plugins/SneakyDB/config.yml`. The file is read once at proxy start; restart the proxy to apply edits. A `maintenance:` section is ignored on the proxy. If the file cannot be read (for example invalid YAML), SneakyDB logs a warning naming the file and the error class (never the file content) and does not connect: readiness fails as for a bad adapter.

### Velocity consumers

Use the same API as on Paper: `SneakyDBProvider.getApi()`, then `whenReady()`, `query`, `execute`, `transaction` and `listen`, with the same [readiness and lifecycle contract](#readiness-and-lifecycle-contract). Declare the dependency in your `velocity-plugin.json` so SneakyDB loads first and its classes are visible to your plugin:

```json
"dependencies": [{ "id": "sneakydb", "optional": false }]
```

Compile against `sneakydb-api` (`compileOnly`, as on Paper) and do not shade it.

Call `getApi()` from your `ProxyInitializeEvent` handler at normal order (the default) or later, or any time after that. SneakyDB subscribes to `ProxyInitializeEvent` with `priority = Short.MAX_VALUE`, ahead of every `PostOrder`, and registers the provider there with readiness pending. Before that event, including in your plugin's constructor, `getApi()` throws `IllegalStateException("SneakyDB is not loaded")`. A consumer handler at `Short.MAX_VALUE` itself is not guaranteed to run after SneakyDB.

```java
@Subscribe
public void onProxyInitialize(ProxyInitializeEvent event) {
    SneakyDBApi database = SneakyDBProvider.getApi();
    database.whenReady()
        .thenCompose(ignored -> database.execute("CREATE TABLE IF NOT EXISTS ...", java.util.List.of()))
        .whenComplete((count, failure) -> { /* runs on a SneakyDB thread */ });
}
```

SneakyDB shuts down on `ProxyShutdownEvent` with `priority = Short.MIN_VALUE`, after consumers' own shutdown handlers, with the same guarantees as Paper's disable: the provider is unregistered, pending readiness and operations fail, and nothing waits on the shutting-down thread. LISTEN/NOTIFY works as on Paper, on the same `SneakyDB-Notifications` thread.

### Differences from Paper

- **No maintenance fallback and no health checks.** A proxy has no CMI, so a database outage or failed startup does not close player access; the background health check exists only to trigger that fallback and is not started. The proxy therefore opens no extra health-check connection.
- Initialization failure behaves otherwise as on Paper: readiness completes exceptionally, one credential-safe error line is logged (`SneakyDB initialization failed during …`), there is no retry, and the proxy keeps running. Fix the config or database and restart the proxy; Velocity cannot reload a single plugin.
- There is no server thread: consumers schedule their own follow-up work (for example with Velocity's scheduler) instead of Bukkit's.

## Adapter design

Adapters are selected and constructed inside SneakyDB. Each adapter supplies a pooled JDBC data source; consumers continue to call `query` and `execute` without depending on that adapter. Adding another database means adding its driver and adapter implementation, and registering its adapter identifier. SQL stays portable only to the extent that the caller writes portable SQL.

## Build and publish

The plugin uses the EmptyPlugin Gradle wrapper and Team-Sneakymouse versioning. Builds require a git checkout with at least one commit. `./gradlew build` creates the plugin jar (Paper and Velocity, `sneakyrp.deployment.type` `paper-plugin,velocity-plugin`); Maven publications are `sneakydb` (server plugin) and `sneakydb-api` (consumer compile-time API). Publish to `https://maven.sneakyrp.com/sneakymouse` through the repository's configured GitHub Actions workflow.
