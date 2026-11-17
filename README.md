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

Existing `query(String, List)`, `execute(String, List)`, parameter binding, and row shapes are unchanged. `transaction(Function)` and the `SneakyDBTransaction` interface are additions (see [Transactions](#transactions)). `transaction` has a default body that throws `UnsupportedOperationException`, so existing custom implementations and test doubles of `SneakyDBApi` keep compiling; override it in a test double to exercise transaction code. A consumer that calls `transaction` needs a server running this SneakyDB version or newer; on an older server the call fails with `NoSuchMethodError`. `whenReady()` adds a Java/JDK-only method; rebuild custom implementations of `SneakyDBApi` and run consumers requiring it with a matching/newer server API. Older consumers can still use the original methods, but pre-readiness calls now fail deterministically rather than racing initialization. `migrate(String, Class)` is an addition with the same kind of default body (`UnsupportedOperationException`); a consumer that calls it needs a server running this SneakyDB version or newer, and fails with `NoSuchMethodError` on an older one. Servers that run this version hold their startup (Paper) or proxy initialisation (Velocity) until the startup migration run has finished; with no plugin calling `migrate` that takes no time. Do not shade the API or depend on internal session/adapter classes.

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

The future completes with the plugin's schema version once its scripts are applied and, under the migration lock, its rows in the version table match its jar. When nothing is pending, that check is all that happens, but it waits for the migration lock like any other step, so the future can wait behind a script that another server on the same database is running. Do no work on the plugin's own tables before that; [Java startup](#java-startup-readiness--schema--reconciliation) shows the whole sequence. SneakyDB runs the scripts once every plugin has enabled, while the server waits for them (see [Startup](#for-operators)), so don't wait for the future inside `onEnable`. A plugin that registers later, for example when it is re-enabled, gets its pending scripts run as soon as the migration thread is free, without holding anything; plugins that register while it is busy are planned together as one batch. A requirement on a plugin that registered after this one and whose scripts haven't run yet fails with advice to restart the server, which plans both together. A restart doesn't help when the other plugin always registers after startup: it has to register during its enable. The future fails, with a message that says what to do, when:

- the scripts are missing, misnamed, misnumbered or not UTF-8, a `-- requires` line doesn't parse, or a script controls the transaction itself (see the rules below);
- the database is newer than the build (its version is higher than the jar's highest script). SneakyDB checks this under the lock before each script and again before the future completes, so it also catches another server that applies newer scripts while this one starts: every applied version of the plugin must exist in the jar with the same checksum, and none may be higher. The check ends there: a server that passed it keeps running when another server applies newer scripts afterwards (see **Rollouts** under [For operators](#for-operators));
- an applied script was edited since (its checksum differs from the one recorded, or from the one another server just recorded for an operator row);
- a requirement on another plugin can't be met, or the requirements form a cycle;
- a script fails (it is rolled back), or another plugin's script failed while running, which stops the run;
- the database is unavailable, or on a server in `verify` mode, scripts are not applied yet.

What the server does after a failure is described under [A failed startup](#for-operators).

**Requirements on other plugins.** When a script needs another plugin's schema at some version, say so in the comment block at the top of the script, before the first SQL statement:

```sql
-- Moves plots from accounts to characters.
-- requires: sneakycharacters >= 3
-- requires-if-present: sneakyusermeta >= 5
alter table plots add column character_id uuid references characters (id);
```

`requires` means the other plugin's database version must be at least that before this script runs; if neither the database nor the scripts registered on this server reach it, the plugin fails. `requires-if-present` is the same, but ignored when the other plugin is absent: it has not registered on this server in this enable generation (a plugin whose migrations failed counts as registered) and has no row in the version table. An ignored requirement is checked again under the lock right before the script runs: if another server has applied the other plugin's scripts meanwhile and they don't reach the version, the plugin fails, and a restart plans again with the other plugin present. Each script also requires its own plugin's previous script.

In the comment block at the top, a comment line is a directive when its first word starts with `requir` (any case, whatever follows it: `requires_if_present`, `required`, `requirement`) and the line holds something shaped like `<plugin> >= <version>` (any comparison operator), or when it is `requires`, `requires-if-present`, `require` or `require-if-present` in any case followed by a colon. A directive that doesn't parse (`-- requires sneakycharacters >= 3`, `-- require: sneakycharacters >= 3`, `-- Requires: …`, `-- required: core >= 1`, `-- requires if present: core >= 1`, `>` instead of `>=`), or one after the first SQL statement, fails the plugin, so a typo can't drop a dependency silently. Comments without such a comparison, such as `-- requires the index above` or `-- Requirements: none`, are prose.

SneakyDB sorts every pending script across all plugins into one order that satisfies the requirements (ties by plugin id, then version) and runs them one after another. A requires line that the database doesn't satisfy yet joins the script and the other plugin's pending scripts into a group, whether or not those scripts reach the required version. A requirement the database already satisfies imposes no order and joins nothing. If one requirement in a group can't be met, nothing in that group runs, not even the scripts of the plugin it names. That makes coordinated changes safe. When two plugins change a shared relation in interleaved steps (plugin A script 5 drops a foreign key, plugin B script 3 retypes the column, A script 6 re-adds the key), declare both sides: B's script 3 has `-- requires: a >= 5`, A's script 6 has `-- requires: b >= 3`. If only one of the two pull requests is merged and deployed, the order is unresolvable, so neither plugin's new scripts run and both keep their previous schema. Plugins outside the group still migrate.

**Rules.**

- Each script runs in its own transaction, together with the row that records it, so it is applied fully or not at all. Write nothing that can't run inside a transaction, such as `CREATE INDEX CONCURRENTLY`, `VACUUM` or `ALTER TYPE … ADD VALUE` on PostgreSQL before 12.
- A script must not control the transaction (`BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT` and the like). What protects the database is the check after each script: SneakyDB sets a savepoint with a new random name before the script and compares the transaction before and after it. If the script ended its transaction, SneakyDB notices right after it, or, when the script failed afterwards, as soon as it can't confirm that the script was still in its transaction. It then records nothing, discards the connection and fails the plugin with a message that the database may hold part of the script's changes: check and repair those by hand before restarting.
- Before that, when the plugin registers, a courtesy check catches the common mistake of wrapping a script in `BEGIN; … COMMIT;`: a top-level `BEGIN`, `START TRANSACTION`, `COMMIT`, `END`, `ROLLBACK`, `ABORT`, `SAVEPOINT`, `RELEASE`, `PREPARE TRANSACTION`, `COMMIT PREPARED` or `ROLLBACK PREPARED` fails the plugin before its scripts run. It is not a full SQL parser: it skips comments, strings, quoted identifiers and dollar-quoted bodies, so `BEGIN … END` in a function body or `DO` block is fine, and it treats every non-ASCII character as part of an identifier. A SQL-standard function body (`BEGIN ATOMIC … END`) passes the check too; as in PostgreSQL, the body ends at the `END` that starts a statement inside it, so `CASE … END` and a column named `end` don't end it. The PostgreSQL JDBC driver runs such a body only as the last statement of a script; anywhere else the script fails and is rolled back, so prefer dollar-quoted bodies.
- Scripts run with `standard_conforming_strings` on, PostgreSQL's default, so a backslash in `'…'` is an ordinary character. Before each script SneakyDB checks the setting on the connection that will run it and fails the plugin without running anything when it is off. The usual cause is a setting on the database or the role (`alter database … set standard_conforming_strings = off`, or the same with `alter role`; psql's `\drds` lists them); the message names both and gives the `reset` statements to run as the database owner or a superuser. Don't turn it off in a script: the statement after that usually fails with a syntax error, and the script is rolled back.
- A script is a whole multi-statement file run as written, without parameters (`$1` or `?` are not bound).
- Use `SET LOCAL` for settings a script needs. SneakyDB runs `RESET ALL` after each script so a plain `SET` doesn't leak into the shared connection pool, but `SET ROLE` and temporary tables would outlive the script.
- Each script waits at most `migrations.lock-timeout-seconds` (default 10) for a table lock and then fails, instead of freezing other servers that share the database. There is no statement timeout: a long data migration runs to the end, and at startup the server waits for it.
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
```

In `verify` mode SneakyDB writes nothing (no table creation, no checksum fill-in); a plugin whose scripts are not all applied fails, and the strict-version and checksum checks still apply. So deploy a new build to the `apply` server first, then restart the `verify` servers. A `verify` server that starts before the `apply` server has finished applying the new scripts fails those plugins (Paper: they stay inactive and CMI maintenance goes on; Velocity: the proxy shuts down) until it is restarted, so start the `apply` server first and wait for its summary line. A value other than `apply` or `verify` runs `verify` and logs a warning. `lock-timeout-seconds` is clamped to 1–300. A `login-message` key from an earlier version is ignored. Several `apply` servers starting at the same time don't apply a script twice: a PostgreSQL advisory lock lets one server at a time apply a script, and each re-checks under the lock what is already applied, skips what another server did meanwhile, and fails a plugin whose applied history no longer matches its jar (a newer version, a different checksum, a `requires` no longer met, a plugin that is no longer absent). These checks happen while a server applies scripts; nothing re-checks a server that is already running (see **Rollouts**).

**Rollouts.** The strict-version check is made at startup, under the lock. A server that passed it, even a moment ago, keeps running with its build when another server applies newer scripts afterwards; SneakyDB neither stops nor re-checks it. So before a rollout whose scripts change a schema that running servers use, stop every server on that database, start the `apply` server with the new builds and wait for its summary line, then start the others. This is an operator rule; SneakyDB doesn't enforce it.

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

**Startup.** The startup run covers every plugin that registered during enable. While it runs, the server waits, so no player can connect before it has ended:

- Paper: in `ServerLoadEvent` with load type `STARTUP` (monitor priority), on the server thread. Paper fires it once every plugin has enabled and the worlds are loaded, right before it lets its network channels accept connections and before the first tick. Until then the listening socket reads nothing, so players can't even reach a login. Paper's watchdog doesn't watch this phase: it starts counting at the first tick, so a run that takes minutes doesn't trip it. A reload fires the event with `RELOAD`, which starts nothing.
- Velocity: in a second `ProxyInitializeEvent` handler at `Short.MIN_VALUE`, after every other plugin's handler. Velocity binds its listener only after that event has completed, blocking handlers included.

The wait covers waiting for the database, waiting for another server's migration lock, and every script. It ends when the run ends, or when SneakyDB shuts down: on Paper, a stop signal (Ctrl-C, `docker stop`) makes Paper disable plugins from its shutdown thread after a second, which ends the wait; a `stop` typed into the console (what a panel's stop button usually sends) only runs after the wait, because console commands run on the server thread. Killing the process ends everything at once, and PostgreSQL rolls back a script that was running. A proxy that gets a stop signal during the wait just exits: its console and its shutdown handling only start after initialisation, so no shutdown event is fired and PostgreSQL rolls back a script that was running. A wait that never ends (the database host vanished mid-script, or another server holds the migration lock and is stuck) has no time limit of its own: send a stop signal or kill the process. Players get no message during the wait; the server just isn't accepting connections yet. When no plugin registered, there is nothing to wait for; if the database connection failed, the [outage fallback](#database-outage-maintenance-fallback) (CMI maintenance at the first tick on Paper) protects the server, as it did before migrations existed. Plugins that register after the startup run (re-enable, a `ProxyInitializeEvent` handler at `Short.MIN_VALUE` that runs after SneakyDB's) run in late batches, which never hold anything. A SneakyDB that is itself enabled after startup (re-enable, `/reload`) runs what registers during that enable at the first tick, as a late batch.

**The migration thread.** Scripts run one after another from one `SneakyDB-Migrations` thread, which also runs the completion callbacks of `migrate()` futures that don't name an executor. A callback that blocks delays the remaining scripts and the end of the startup wait; keep callbacks short or hand the work to another thread. During the startup run the server thread is waiting: a callback that hands work to it (`runTask`) is fine and runs once startup continues, but a callback that waits until the server thread has done something deadlocks the server.

**Log lines.** Each applied script logs one info line (`Applied migration sneakyplots:V002__plot_character.sql in 35 ms.`) and each run a summary (`Migrations (startup, apply mode): 3 scripts applied, 4 of 4 plugins ready in 412 ms.`). Each failed plugin logs one severe line that names the script and the reason; for a failed script that includes PostgreSQL's SQL state and message:

```
Migrations for sneakyplots failed at V003__plot_owner.sql: SQL state 42703: ERROR: column "owner" does not exist
```

A script that fails while running is followed by a severe summary instead of the info one:

```
Migrations (startup, apply mode) stopped: sneakyplots:V003__plot_owner.sql failed while running, so no further script ran; 2 scripts applied, 1 of 4 plugins ready; failed: sneakyplots, sneakyshops, sneakyusermeta. The server stops so the error can't compound; fix the cause logged above and start it again (each start retries the script) (in 412 ms).
```

Connection and configuration failures name only the error class and SQL state, as elsewhere in SneakyDB.

**A failed startup.** There are two kinds of failure, and the server reacts differently:

- **A script failed while running**: its SQL failed, it hit `lock-timeout-seconds` (SQL state `55P03`), the connection was lost during it, or SneakyDB could not confirm that it stayed in its transaction. The run stops at once: no further script of any plugin runs, every plugin that hasn't completed fails, and the server stops (Paper: an orderly shutdown; Velocity: the proxy shuts down). SneakyDB switches no maintenance on, and cancels a maintenance command that a failed health check queued during the wait, so there is nothing to undo after the fix. A plugin that registered while the run was going (from another thread, or from a task scheduled for the first tick) fails too and runs nothing. A stopped server means: read the two severe lines before the shutdown (the failed script with its reason, then the summary). The failed script was rolled back and recorded nothing, unless the message says the database may hold part of its changes; scripts that ran before it stay applied. The process exits with status 0 on both platforms. A panel that restarts stopped servers will start it again, and each start retries the script; that is harmless for a script error that the data doesn't fix by itself, but it keeps stopping until the cause is fixed.
- **A plugin failed outside a running script**: an unresolvable requirement or a cycle in its group, a jar older than the database, a checksum mismatch, a malformed or rejected script, a `verify` server with pending scripts, the missing `sneakydb` schema or privileges, failed database initialisation, a re-check under the lock that says `restart the server`. Nothing was running when it failed, so nothing is half applied; scripts of that plugin that ran earlier in the same run stay applied and recorded. Its future fails and it stays inactive (it never gets its ready signal); SneakyDB doesn't disable it or refuse its other calls. Unrelated plugins still migrate. On Paper, when the run is over, SneakyDB sends `cmi maintenance true <maintenance.message>` on the server thread before startup continues (see [the fallback](#database-outage-maintenance-fallback)), and logs a severe line that says so; then the server starts. On Velocity, which has no such fallback, the proxy shuts down.

When both happen in one run, the server stops. A startup run without failures changes nothing. Late batches never stop the server or switch maintenance on: their failures fail those plugins and are logged, and after a script fails while running nothing further of that batch runs.

To recover:

1. Read the severe lines. Typical causes and fixes: a script error (ship a build with a corrected *new* script, or fix the data it tripped over); `the database is newer than this build` (install the plugin build that matches the database); a checksum mismatch (restore the original script in the build; only if the edit was deliberate and the database already matches it, set that row's `checksum` to null so SneakyDB records the new one); an unmet requirement (deploy the other plugin's matching build too); missing schema or privileges (run the statements from the log); a lock timeout, SQL state `55P03` (find what held the lock, then restart); `the connection to the database was lost while it ran` (PostgreSQL rolled the script back; find out why the connection ended, then restart); `ended the transaction SneakyDB runs it in` or `could not confirm that it was still in the transaction` (repair the script's partial changes by hand and ship a build without the transaction control); `standard_conforming_strings is off` (run the `reset` statements from the message, then restart); a message that says `restart the server` (another server changed the version table while this one started; a restart plans again).
2. Start the server again (or, after a failure before the scripts ran, restart it). Pending scripts run again from where they stopped.
3. After a failure before the scripts ran, Paper switched CMI maintenance on: switch it off by hand once the server is healthy (`cmi maintenance false`); SneakyDB never switches it off.

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

Startup initialization failure, a plugin failing before any of its scripts ran in the startup migration run (sent before startup continues; see [A failed startup](#for-operators)), or the first unsuccessful background connectivity check after successful initialization, requests **`cmi maintenance true <message>`** on the server thread. This is an explicit enable, never the argument-free toggle. The command contract comes from [CMI's official command reference](https://www.zrips.net/cmi/commands/) (`maintenance (true/false) (message)`, bypass permission `cmi.command.maintenance.bypass`). CMI owns player admission, bypass rules, message formatting, and any effects on already-connected players.

```yaml
maintenance:
  message: 'Database unavailable. Please try again later.'
  health-check-interval-seconds: 30
```

The message applies to both startup and runtime outages. Blank, control-character-containing, or over-512-character messages use the shown default. The interval is clamped to 5–300 seconds and is a delay **after** a completed check; the first check starts immediately after readiness. Config is read on enable; restart or disable/re-enable to apply edits (no hot reload command).

Health checks use a dedicated single-thread scheduler and a separate one-connection PostgreSQL pool, not the consumer worker queue or consumer pool. Allow **one additional database connection** beyond `database.maximum-pool-size`. Acquisition, driver connect/login/socket waits, and `Connection.isValid(5)` validation have five-second bounds; these are individual JDBC/driver bounds, not a hard end-to-end deadline. Checks do not overlap or accumulate, and shutdown fences queued main-thread effects and closes health resources off-thread. Consumer SQL syntax/constraint errors, worker rejection, and consumer pool contention alone do not activate maintenance.

**No automatic reopening:** only one maintenance command is attempted per enable generation, including across recovery and subsequent outages. Neither recovery nor disable/re-enable sends `false` or a toggle, so existing administrator maintenance stays enabled; an outage may replace its player-facing message. Operators must verify the database and manually reopen using CMI after recovery. A failed initial startup remains terminal: the health loop does not retry initialization. Fix configuration/access and restart or disable/re-enable first. The Java consumer API, historical `whenReady()`, and lifecycle-only `isAvailable()` semantics are unchanged.

**CMI limitation:** CMI is an optional, ordered server dependency, accessed by command without a joined classpath. If CMI is missing/disabled, or dispatch fails, SneakyDB emits a credential-safe severe diagnostic and does not claim the server is protected. Apart from holding startup while the [startup migrations](#for-operators) run, there is **no independent login/kick guard**, no automatic command retry, and no proof of admission policy from Bukkit's command-dispatch boolean. Installing/enabling CMI later requires a new SneakyDB enable generation or manual maintenance activation. Ensure CMI maintenance is available and correctly configured before relying on this fallback; close access manually if it is unavailable. This integration is unit/MockBukkit verified, not a live CMI-server test.

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
```

The keys, defaults and bounds are the same as the Paper `database:` and `migrations:` sections, so copy the values from a server's `plugins/SneakyDB/config.yml`. A proxy that shares its database with a server that runs `apply` should run `verify`. The file is read once at proxy start; restart the proxy to apply edits. A `maintenance:` section is ignored on the proxy. If the file cannot be read (for example invalid YAML), SneakyDB logs a warning naming the file and the error class (never the file content) and does not connect: readiness fails as for a bad adapter.

### Velocity consumers

Use the same API as on Paper: `SneakyDBProvider.getApi()`, then `migrate` or `whenReady()`, `query`, `execute`, `transaction` and `listen`, with the same [readiness and lifecycle contract](#readiness-and-lifecycle-contract). Declare the dependency in your `velocity-plugin.json` so SneakyDB loads first and its classes are visible to your plugin:

```json
"dependencies": [{ "id": "sneakydb", "optional": false }]
```

Compile against `sneakydb-api` (`compileOnly`, as on Paper) and do not shade it.

Call `getApi()` from your `ProxyInitializeEvent` handler at normal order (the default) or later, or any time after that. SneakyDB subscribes to `ProxyInitializeEvent` with `priority = Short.MAX_VALUE`, ahead of every `PostOrder`, and registers the provider there with readiness pending. Before that event, including in your plugin's constructor, `getApi()` throws `IllegalStateException("SneakyDB is not loaded")`. A consumer handler at `Short.MAX_VALUE` itself is not guaranteed to run after SneakyDB.

Call `migrate` from that same handler. SneakyDB runs the startup migrations from a second `ProxyInitializeEvent` handler at `priority = Short.MIN_VALUE`, after every handler at a `PostOrder`, and blocks in it until they have ended, so the proxy accepts no connections meanwhile; a registration from a handler at `Short.MIN_VALUE` itself, or from an asynchronous continuation that finishes after the event, may miss the startup run and is then handled as a late registration.

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
- **Any failed startup run shuts the proxy down.** Where Paper switches CMI maintenance on and continues (a plugin failed before any of its scripts ran), the proxy shuts down too, as it does after a script failed while running; SneakyDB logs a severe line that says why. Fix the cause and start the proxy again.
- Initialization failure behaves otherwise as on Paper: readiness completes exceptionally, one credential-safe error line is logged (`SneakyDB initialization failed during …`) and there is no retry. With no plugin registered for migrations the proxy keeps running; with one, the failed startup run shuts it down. Fix the config or database and restart the proxy; Velocity cannot reload a single plugin.
- There is no server thread: consumers schedule their own follow-up work (for example with Velocity's scheduler) instead of Bukkit's.

## Adapter design

Adapters are selected and constructed inside SneakyDB. Each adapter supplies a pooled JDBC data source; consumers continue to call `query` and `execute` without depending on that adapter. Adding another database means adding its driver and adapter implementation, and registering its adapter identifier. SQL stays portable only to the extent that the caller writes portable SQL.

## Build and publish

The plugin uses the EmptyPlugin Gradle wrapper and Team-Sneakymouse versioning. Builds require a git checkout with at least one commit. `./gradlew build` creates the plugin jar (Paper and Velocity, `sneakyrp.deployment.type` `paper-plugin,velocity-plugin`); Maven publications are `sneakydb` (server plugin) and `sneakydb-api` (consumer compile-time API). Publish to `https://maven.sneakyrp.com/sneakymouse` through the repository's configured GitHub Actions workflow.
