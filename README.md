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
- **Shutdown:** pending readiness and outstanding query/execute/transaction/migrate futures fail exceptionally. Queued database operations are discarded; running JDBC operations are interrupted best-effort and may already have affected the database. For `execute`, an exceptional future is not proof of rollback. A transaction stays all-or-nothing: shutdown makes its next handle call throw and SneakyDB rolls it back instead of committing, or the pool closure aborts its connection and PostgreSQL discards the uncommitted work. Only a commit already in flight when shutdown arrives can still succeed after its future failed. Shutdown never waits for workers, transactions or pool closure on the server thread. A pool produced by late initialization is closed, never published.
- **Stale handles:** APIs belong to one enable generation. Old handles never attach to a new generation. Reacquire the provider API for a new enable. The provider is registered while initialization is pending and unavailable while disabled.
- **Operations:** `query`/`execute`/`transaction` fail fast via an exceptional future if called before readiness, after initialization failure, or after shutdown; they do not wait for initialization. Worker rejection (shutdown or overload) also returns an exceptional future, not a synchronous executor exception. Four workers and a queue of 256 bound submitted work.

Connection acquisition uses the configured `connection-timeout-ms` (clamped to 250–30000 ms). PostgreSQL connection/login timeouts round that value up to whole seconds, and validation uses the finite timeout above. These are driver/pool bounds, not an exact end-to-end startup deadline. SQL execution has no new statement timeout. Never `join()`, `get()`, sleep, or poll on the Minecraft thread. Non-async completion callbacks may run inline on the subscriber or completing thread; keep them short or select an appropriate executor. Consumer callback code must not block shutdown.

### Java startup: readiness → schema → reconciliation

Dependency load order makes the provider accessible, not database-ready. Sequence schema setup and initial reconciliation explicitly. The plugin's schema comes from its own migration scripts (see [Migrations](#migrations)): `migrate` completes once the database is ready and the plugin's scripts are applied, so it replaces both `whenReady()` and any `CREATE TABLE IF NOT EXISTS` at startup. For example, in a consumer `JavaPlugin`:

```java
// Field; increment on the server thread in onEnable AND onDisable.
private volatile long startupGeneration;

@Override public void onEnable() {
    long generation = ++startupGeneration;
    SneakyDBApi database = SneakyDBProvider.getApi();
    database.migrate(IDENTIFIER, getClass()) // IDENTIFIER: the plugin's lower-case id, e.g. "sneakyplots"
        .thenCompose(version -> database.query(
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

If reconciliation itself needs more database writes, compose their futures too; do not assume submission means completion. The fence protects Bukkit effects, not database writes already in flight. Consumers needing transactional cancellation must implement their own database-level protocol. A plugin without tables of its own skips `migrate` and starts from `whenReady()` instead.

### Compatibility

Existing `query(String, List)`, `execute(String, List)`, parameter binding, and row shapes are unchanged. `transaction(Function)` and the `SneakyDBTransaction` interface are additions (see [Transactions](#transactions)). `transaction` has a default body that throws `UnsupportedOperationException`, so existing custom implementations and test doubles of `SneakyDBApi` keep compiling; override it in a test double to exercise transaction code. A consumer that calls `transaction` needs a server running this SneakyDB version or newer; on an older server the call fails with `NoSuchMethodError`. `whenReady()` adds a Java/JDK-only method; rebuild custom implementations of `SneakyDBApi` and run consumers requiring it with a matching/newer server API. Older consumers can still use the original methods, but pre-readiness calls now fail deterministically rather than racing initialization. `migrate(String, Class)` is an addition with the same kind of default body (`UnsupportedOperationException`); a consumer that calls it needs a server running this SneakyDB version or newer, and fails with `NoSuchMethodError` on an older one. Servers that run this version refuse logins until the startup migration run has finished, even when no plugin calls `migrate` (then only until the first server tick). Do not shade the API or depend on internal session/adapter classes.

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

## Migrations

A plugin owns its schema: it ships numbered SQL scripts in its jar, and SneakyDB runs the pending ones at server startup until the database matches the plugin. Every database (Dev, Staging, Live) records what is applied in one table, `sneakydb.schema_migrations`.

### For plugin authors

Put the scripts in `src/main/resources/db/migrations/`, so they end up in the jar under `db/migrations/`:

```
db/migrations/V001__init.sql
db/migrations/V002__plot_character.sql
```

- Names are `V<number>__<name>.sql`: a capital `V`, the number, two underscores, a name of letters, digits, `_` or `-`. Numbers start at 1 and have no gaps or duplicates (`V1` and `V001` are the same number); leading zeros are optional and only sort the files. Every file in the folder must be a script: any other file name fails the plugin, so a typo can't hide a script.
- The plugin's schema version is its highest script number.
- Scripts are UTF-8. Line endings and a leading BOM don't matter (the checksum normalises them), so a Windows checkout gives the same checksum as a Linux one.

Register once per enable, during enable, with the plugin's stable id (its `IDENTIFIER`, lower case: `[a-z][a-z0-9_]{0,62}`) and any class from the plugin's own jar:

```java
database.migrate(IDENTIFIER, getClass()).whenComplete((version, failure) -> {
    // Runs on a SneakyDB thread. failure != null: the plugin's tables are not usable; the message says why.
});
```

```kotlin
SneakyDBProvider.getApi().migrate(IDENTIFIER, javaClass).whenComplete { version, failure -> /* SneakyDB thread */ }
```

The future completes with the plugin's schema version once its scripts are applied and, under the migration lock, its rows in the version table match its jar; when nothing is pending that happens as soon as the run starts. Do no work on the plugin's own tables before that; [Java startup](#java-startup-readiness--schema--reconciliation) shows the whole sequence. SneakyDB runs the scripts only after every plugin had its chance to register (the first server tick on Paper, the end of proxy initialisation on Velocity), so don't wait for the future inside `onEnable`. A plugin that registers later, for example when it is re-enabled, gets its pending scripts run right away. The future fails, with a message that says what to do, when:

- the scripts are missing, misnamed, misnumbered or not UTF-8, a `-- requires` line doesn't parse, or a script controls the transaction itself (see the rules below);
- the database is newer than the build (its version is higher than the jar's highest script): an older jar never runs against a schema that scripts may have changed under it. That holds when another server applies newer scripts while this one starts, too: before each script, and before the future completes, SneakyDB checks under the lock that every applied version of the plugin exists in the jar with the same checksum and none is higher;
- an applied script was edited since (its checksum differs from the one recorded, or from the one another server just recorded for an operator row);
- a requirement on another plugin can't be met, or the requirements form a cycle;
- a script fails (it is rolled back), or a script it depends on failed;
- the database is unavailable, or on a server in `verify` mode, scripts are not applied yet.

**Requirements on other plugins.** When a script needs another plugin's schema at some version, say so in the comment block at the top of the script, before the first SQL statement:

```sql
-- Moves plots from accounts to characters.
-- requires: sneakycharacters >= 3
-- requires-if-present: sneakyusermeta >= 5
alter table plots add column character_id uuid references characters (id);
```

`requires` means the other plugin's database version must be at least that before this script runs; if neither the database nor the scripts registered on this server reach it, the plugin fails. `requires-if-present` is the same, but ignored when the other plugin is absent: it has not registered on this server in this enable generation (a plugin whose migrations failed counts as registered) and has no row in the version table. An ignored requirement is checked again under the lock right before the script runs: if another server has applied the other plugin's scripts meanwhile and they don't reach the version, the plugin fails, and a restart plans again with the other plugin present. Each script also requires its own plugin's previous script.

A comment line is a directive when its first word is `requires` or `requires-if-present` followed by a colon, or by something shaped like `<plugin> >= <version>` (a forgotten colon). A directive that doesn't parse (`-- requires sneakycharacters >= 3`, `-- Requires: …`, `>` instead of `>=`), or one after the first SQL statement, fails the plugin, so a typo can't drop a dependency silently. Other comments that start with the word, such as `-- requires the index above` or `-- Requirements: …`, are prose.

SneakyDB sorts every pending script across all plugins into one order that satisfies the requirements (ties by plugin id, then version) and runs them one after another. A pending script and the pending scripts of every plugin its requires lines name form a group, whether or not those scripts reach the required version: if one requirement in a group can't be met, nothing in that group runs, not even the scripts of the plugin it names. That makes coordinated changes safe. When two plugins change a shared relation in interleaved steps (plugin A script 5 drops a foreign key, plugin B script 3 retypes the column, A script 6 re-adds the key), declare both sides: B's script 3 has `-- requires: a >= 5`, A's script 6 has `-- requires: b >= 3`. If only one of the two pull requests is merged and deployed, the order is unresolvable, so neither plugin's new scripts run and both keep their previous schema. Plugins outside the group still migrate.

**Rules.**

- Each script runs in its own transaction, together with the row that records it, so it is applied fully or not at all. Write nothing that can't run inside a transaction, such as `CREATE INDEX CONCURRENTLY`, `VACUUM` or `ALTER TYPE … ADD VALUE` on PostgreSQL before 12.
- A script must not control the transaction. A top-level `BEGIN`, `START TRANSACTION`, `COMMIT`, `END`, `ROLLBACK`, `ABORT`, `SAVEPOINT`, `RELEASE`, `PREPARE TRANSACTION`, `COMMIT PREPARED` or `ROLLBACK PREPARED` fails the plugin before any of its scripts run (the check skips comments, strings, quoted identifiers and dollar-quoted bodies, so `BEGIN … END` in a function body or `DO` block is fine). A SQL-standard function body (`BEGIN ATOMIC … END`) passes the check too, but the PostgreSQL JDBC driver runs one only as the last statement of a script; anywhere else the script fails and is rolled back, so prefer dollar-quoted bodies. If a script still ends its transaction, SneakyDB notices right after it, records nothing and fails the plugin with a message that the database may hold part of the script's changes: check and repair those by hand before restarting.
- A script is a whole multi-statement file run as written, without parameters (`$1` or `?` are not bound).
- Use `SET LOCAL` for settings a script needs. SneakyDB runs `RESET ALL` after each script so a plain `SET` doesn't leak into the shared connection pool, but `SET ROLE` and temporary tables would outlive the script.
- Each script waits at most `migrations.lock-timeout-seconds` (default 10) for a table lock and then fails, instead of freezing other servers that share the database. There is no statement timeout: a long data migration runs to the end, with logins refused meanwhile.
- **Never edit a merged script.** Its checksum is recorded; a changed script fails the plugin on every database that applied the original. Put fixes in a new script.
- Forward only: there are no down migrations. To undo a change, write a new script that reverses it.
- Don't create or drop schemas in scripts; create tables in the schema the plugin already uses.

In tests that run from class directories (Gradle, MockBukkit), SneakyDB finds `db/migrations` next to the anchor class, or else in the one directory on the anchor's class path that has it (Gradle's `build/resources/main`).

### For operators

**One-time setup per database.** SneakyDB never creates the `sneakydb` schema. As the database owner or a superuser, run once for each database, with the role in the servers' `database.username`:

```sql
create schema sneakydb;
grant usage, create on schema sneakydb to <role>;
```

SneakyDB creates `sneakydb.schema_migrations` itself the first time a server in `apply` mode starts. Without the schema, or without those privileges, every plugin that registered fails and the log gives the statements to run. If you create the table by hand as another role (see [existing tables](#for-operators)), the servers' role owns nothing on it, so grant it what SneakyDB needs: `grant select, insert, update on sneakydb.schema_migrations to <role>;` (`update` fills in checksums of operator rows; `verify` servers only need `select`).

**Modes.** One server per database runs `apply`; every other server on that database runs `verify`:

```yaml
migrations:
  # apply: run pending scripts. verify: never change the database; a plugin whose scripts are not all applied fails.
  mode: apply
  lock-timeout-seconds: 10
  # Shown to players who try to join while migrations run or after one failed.
  login-message: 'The server is starting. Please try again in a moment.'
```

In `verify` mode SneakyDB writes nothing (no table creation, no checksum fill-in); a plugin whose scripts are not all applied fails, and the strict-version and checksum checks still apply. So deploy a new build to the `apply` server first, then restart the `verify` servers. A `verify` server that starts before the `apply` server has finished applying the new scripts fails those plugins and stays closed until it is restarted, so start the `apply` server first and wait for its summary line. A value other than `apply` or `verify` runs `verify` and logs a warning. `lock-timeout-seconds` is clamped to 1–300. An invalid `login-message` (blank, several lines, over 512 characters) uses the default. Several `apply` servers on one database are safe as well: a PostgreSQL advisory lock lets one server at a time apply a script, and each re-checks under the lock what is already applied, skips what another server did meanwhile, and fails a plugin whose applied history no longer matches its jar (a newer version, a different checksum, a `requires` no longer met, a plugin that is no longer absent).

**Existing tables.** When a plugin starts using migrations on a database whose tables already exist (created by hand or by older `CREATE TABLE IF NOT EXISTS` code), record the scripts that the existing tables already match, with no checksum, before the `apply` server starts with that build:

```sql
create table if not exists sneakydb.schema_migrations (
    plugin text not null, version integer not null, name text not null, checksum text,
    applied_at timestamptz not null default now(), primary key (plugin, version)
);
insert into sneakydb.schema_migrations (plugin, version, name) values ('sneakyplots', 1, 'V001__init.sql');
```

A row with a null checksum counts as applied and is not compared. The first time the plugin registers on an `apply` server, SneakyDB fills in the jar's checksum under the lock (if another server filled in a different one meanwhile, the plugin fails), and from then on the script is protected like any other. The plugin's future completes only after that. Versions must be contiguous from 1.

**Removed plugins.** Rows stay in the version table when a plugin is uninstalled, and a plugin with rows counts as present for `requires-if-present` on every server of that database. Once its tables are gone for good, delete its rows (`delete from sneakydb.schema_migrations where plugin = '<id>';`), so other plugins treat it as absent again.

**Logins.** From SneakyDB's enable until the startup run has finished, SneakyDB refuses logins itself with `login-message`. There is no bypass permission at that stage. It refuses early, so other plugins' handlers can see the refusal, and again at the end, so no other handler can allow the login and a player whose early check ran before SneakyDB enabled is still caught:

- Paper: `AsyncPlayerPreLoginEvent` at the lowest priority, then `PlayerConnectionValidateLoginEvent` at the monitor priority, which Paper fires when the login starts and again when configuration finishes, right before the player joins the world.
- Velocity: `PreLoginEvent` first (`Short.MAX_VALUE`) and last (`Short.MIN_VALUE`), then `LoginEvent` last.

When no plugin registered, logins open at the first server tick (Velocity: at the end of proxy initialisation). Plugins that register later run as their own batch and don't affect logins. On Velocity, a consumer that registers from a `ProxyInitializeEvent` handler at `Short.MIN_VALUE` may run after SneakyDB's second handler and is then handled as such a late registration.

The gate is about migrations only. When no plugin has registered migrations, it opens at the first tick even if the database connection failed; then the [outage fallback](#database-outage-maintenance-fallback) (CMI maintenance on Paper) protects the server, as it did before migrations existed.

**The migration thread.** Scripts run one after another from one `SneakyDB-Migrations` thread, which also runs the completion callbacks of `migrate()` futures that don't name an executor. A callback that blocks delays the remaining scripts and the opening of logins; keep callbacks short or hand the work to another thread.

**Log lines.** Each applied script logs one info line (`Applied migration sneakyplots:V002__plot_character.sql in 35 ms.`) and each run a summary (`Migrations (startup, apply mode): 3 scripts applied, 4 of 4 plugins ready in 412 ms.`). Each failed plugin logs one severe line that names the script and the reason; for a failed script that includes PostgreSQL's SQL state and message:

```
Migrations for sneakyplots failed at V003__plot_owner.sql: SQL state 42703: ERROR: column "owner" does not exist
```

Connection and configuration failures name only the error class and SQL state, as elsewhere in SneakyDB.

**A failed startup.** When any plugin fails in the startup run, logins stay refused for the rest of this enable generation, and on Paper SneakyDB also sends `cmi maintenance true <maintenance.message>` once, exactly as for a database outage (see [the fallback](#database-outage-maintenance-fallback)). The failed script was rolled back; scripts that ran before it stay applied, and the failed plugin's later scripts and every script that depends on them were skipped. Plugins outside the failed group migrated normally. To recover:

1. Read the severe lines. Typical causes and fixes: a script error (ship a build with a corrected *new* script, or fix the data it tripped over); `the database is newer than this build` (install the plugin build that matches the database); a checksum mismatch (restore the original script in the build; only if the edit was deliberate and the database already matches it, set that row's `checksum` to null so SneakyDB records the new one); an unmet requirement (deploy the other plugin's matching build too); missing schema or privileges (run the statements from the log); a lock timeout, SQL state `55P03` (find what held the lock, then restart); `ended the transaction SneakyDB runs it in` (repair the script's partial changes by hand and ship a build without the transaction control); a message that says `restart the server` (another server changed the version table while this one started; a restart plans again).
2. Restart the server, or disable and re-enable SneakyDB together with its dependents. Pending scripts run again from where they stopped.
3. Reopen CMI maintenance by hand once the server is healthy (`cmi maintenance false`); SneakyDB never reopens it.

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

Set credentials for your server before enabling the plugin. The file also has the `maintenance:` section (see [the fallback](#database-outage-maintenance-fallback)) and the `migrations:` section (see [Migrations for operators](#for-operators)). Database connection and query work runs on SneakyDB's worker threads; migration scripts run one at a time through the same workers and pool, from a `SneakyDB-Migrations` thread, and need no extra connection. The PostgreSQL driver and connection pool are bundled in the server jar.

## Database-outage maintenance fallback

Startup initialization failure, a plugin failing in the startup migration run (see [Migrations](#migrations)), or the first unsuccessful background connectivity check after successful initialization, requests **`cmi maintenance true <message>`** on the server thread. This is an explicit enable, never the argument-free toggle. The command contract comes from [CMI's official command reference](https://www.zrips.net/cmi/commands/) (`maintenance (true/false) (message)`, bypass permission `cmi.command.maintenance.bypass`). CMI owns player admission, bypass rules, message formatting, and any effects on already-connected players.

```yaml
maintenance:
  message: 'Database unavailable. Please try again later.'
  health-check-interval-seconds: 30
```

The message applies to both startup and runtime outages. Blank, control-character-containing, or over-512-character messages use the shown default. The interval is clamped to 5–300 seconds and is a delay **after** a completed check; the first check starts immediately after readiness. Config is read on enable; restart or disable/re-enable to apply edits (no hot reload command).

Health checks use a dedicated single-thread scheduler and a separate one-connection PostgreSQL pool, not the consumer worker queue or consumer pool. Allow **one additional database connection** beyond `database.maximum-pool-size`. Acquisition, driver connect/login/socket waits, and `Connection.isValid(5)` validation have five-second bounds; these are individual JDBC/driver bounds, not a hard end-to-end deadline. Checks do not overlap or accumulate, and shutdown fences queued main-thread effects and closes health resources off-thread. Consumer SQL syntax/constraint errors, worker rejection, and consumer pool contention alone do not activate maintenance.

**No automatic reopening:** only one maintenance command is attempted per enable generation, including across recovery and subsequent outages. Neither recovery nor disable/re-enable sends `false` or a toggle, so existing administrator maintenance stays enabled; an outage may replace its player-facing message. Operators must verify the database and manually reopen using CMI after recovery. A failed initial startup remains terminal: the health loop does not retry initialization. Fix configuration/access and restart or disable/re-enable first. The Java consumer API, historical `whenReady()`, and lifecycle-only `isAvailable()` semantics are unchanged.

**CMI limitation:** CMI is an optional, ordered server dependency, accessed by command without a joined classpath. If CMI is missing/disabled, or dispatch fails, SneakyDB emits a credential-safe severe diagnostic and does not claim the server is protected. Apart from the [migration login gate](#for-operators), which only covers startup migrations, there is **no independent login/kick guard**, no automatic command retry, and no proof of admission policy from Bukkit's command-dispatch boolean. Installing/enabling CMI later requires a new SneakyDB enable generation or manual maintenance activation. Ensure CMI maintenance is available and correctly configured before relying on this fallback; close access manually if it is unavailable. This integration is unit/MockBukkit verified, not a live CMI-server test.

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

migrations:
  mode: apply
  lock-timeout-seconds: 10
  login-message: 'The server is starting. Please try again in a moment.'
```

The keys, defaults and bounds are the same as the Paper `database:` and `migrations:` sections, so copy the values from a server's `plugins/SneakyDB/config.yml`. A proxy that shares its database with a server that runs `apply` should run `verify`. The file is read once at proxy start; restart the proxy to apply edits. A `maintenance:` section is ignored on the proxy. If the file cannot be read (for example invalid YAML), SneakyDB logs a warning naming the file and the error class (never the file content) and does not connect: readiness fails as for a bad adapter.

### Velocity consumers

Use the same API as on Paper: `SneakyDBProvider.getApi()`, then `migrate` or `whenReady()`, `query`, `execute`, `transaction` and `listen`, with the same [readiness and lifecycle contract](#readiness-and-lifecycle-contract). Declare the dependency in your `velocity-plugin.json` so SneakyDB loads first and its classes are visible to your plugin:

```json
"dependencies": [{ "id": "sneakydb", "optional": false }]
```

Compile against `sneakydb-api` (`compileOnly`, as on Paper) and do not shade it.

Call `getApi()` from your `ProxyInitializeEvent` handler at normal order (the default) or later, or any time after that. SneakyDB subscribes to `ProxyInitializeEvent` with `priority = Short.MAX_VALUE`, ahead of every `PostOrder`, and registers the provider there with readiness pending. Before that event, including in your plugin's constructor, `getApi()` throws `IllegalStateException("SneakyDB is not loaded")`. A consumer handler at `Short.MAX_VALUE` itself is not guaranteed to run after SneakyDB.

Call `migrate` from that same handler. SneakyDB starts the startup migration run from a second `ProxyInitializeEvent` handler at `priority = Short.MIN_VALUE`, after every handler at a `PostOrder`; a registration from a handler at `Short.MIN_VALUE` itself, or from an asynchronous continuation that finishes after the event, may miss the startup run and then runs as its own batch.

```java
@Subscribe
public void onProxyInitialize(ProxyInitializeEvent event) {
    SneakyDBApi database = SneakyDBProvider.getApi();
    database.migrate(IDENTIFIER, getClass())
        .whenComplete((version, failure) -> { /* runs on a SneakyDB thread */ });
}
```

SneakyDB shuts down on `ProxyShutdownEvent` with `priority = Short.MIN_VALUE`, after consumers' own shutdown handlers, with the same guarantees as Paper's disable: the provider is unregistered, pending readiness and operations fail, and nothing waits on the shutting-down thread. LISTEN/NOTIFY works as on Paper, on the same `SneakyDB-Notifications` thread.

### Differences from Paper

- **No maintenance fallback and no health checks.** A proxy has no CMI, so a database outage or failed initialization does not close player access; the background health check exists only to trigger that fallback and is not started. The proxy therefore opens no extra health-check connection.
- **Login gate.** As on Paper, SneakyDB refuses logins (`PreLoginEvent` first and last, `LoginEvent` last, denied with `migrations.login-message`) from its initialization until the startup migration run has finished, and for the rest of the proxy's run if a plugin failed in it. Without CMI nothing else happens; restart the proxy after fixing the cause.
- Initialization failure behaves otherwise as on Paper: readiness completes exceptionally, one credential-safe error line is logged (`SneakyDB initialization failed during …`), there is no retry, and the proxy keeps running. Fix the config or database and restart the proxy; Velocity cannot reload a single plugin.
- There is no server thread: consumers schedule their own follow-up work (for example with Velocity's scheduler) instead of Bukkit's.

## Adapter design

Adapters are selected and constructed inside SneakyDB. Each adapter supplies a pooled JDBC data source; consumers continue to call `query` and `execute` without depending on that adapter. Adding another database means adding its driver and adapter implementation, and registering its adapter identifier. SQL stays portable only to the extent that the caller writes portable SQL.

## Build and publish

The plugin uses the EmptyPlugin Gradle wrapper and Team-Sneakymouse versioning. Builds require a git checkout with at least one commit. `./gradlew build` creates the plugin jar (Paper and Velocity, `sneakyrp.deployment.type` `paper-plugin,velocity-plugin`); Maven publications are `sneakydb` (server plugin) and `sneakydb-api` (consumer compile-time API). Publish to `https://maven.sneakyrp.com/sneakymouse` through the repository's configured GitHub Actions workflow.
