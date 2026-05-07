# @gatrix/ripple

**Ship data changes to live servers in seconds — not hours.**

The name **Ripple** comes from the way a single stone dropped into water sends ripples outward to every shore. One data change, published once, propagates to every server instance — instantly and reliably.

![hero-banner](docs/hero-banner.png)

---

## Why Ripple?

### The Problem You Already Have

Every live game service hits this wall eventually:

🔴 **"The designers updated the item balance table. We need a server maintenance window."**

A 30-minute maintenance for a single config change. Players are kicked out. Revenue dips. Your Discord fills with complaints. The operations team stays late. Again.

And it's not just balance tables:

| Scenario | Without Ripple | With Ripple |
|----------|---------------|-------------|
| Item/shop config update | 🔴 Server restart required | 🟢 Hot-reload in < 1 second |
| Emergency hotfix for a broken event | 🔴 Schedule maintenance, notify players | 🟢 Publish `event/*` — fixed instantly |
| Localization typo fix | 🔴 Full deployment pipeline | 🟢 `curl -X POST /ripple/refresh -d '{"pattern":"localization/*"}'` |
| A/B test config toggle | 🔴 Redeploy all servers | 🟢 One API call, all servers update simultaneously |
| Multi-server data consistency after DB migration | 🔴 Rolling restart with downtime risk | 🟢 Broadcast `**` — every server reloads every handler |

### How It Works (30-Second Version)

```
Designer updates item table in admin tool
        │
        ▼
  ┌─────────────┐     Redis Stream     ┌─────────────┐
  │ Admin API   │ ──── XADD ────────▶ │  Stream:     │
  │ POST /refresh│                     │  ripple:refresh│
  └─────────────┘                      └──────┬──────┘
                                              │
                          ┌───────────────────┼───────────────────┐
                          ▼                   ▼                   ▼
                   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
                   │ Server A    │    │ Server B    │    │ Server C    │
                   │ XREADGROUP  │    │ XREADGROUP  │    │ XREADGROUP  │
                   │ → reload()  │    │ → reload()  │    │ → reload()  │
                   └─────────────┘    └─────────────┘    └─────────────┘
                   
  ✅ All servers updated simultaneously. Zero downtime. No restart.
```

### Why Not Just Use Redis Pub/Sub?

You might think: *"I can already do this with Pub/Sub."*

You could — until the first time it breaks:

| Concern | Redis Pub/Sub | Ripple (Redis Streams) |
|---------|---------------|----------------------|
| Server crashes mid-refresh | ❌ Message lost forever | ✅ XPENDING + XCLAIM auto-recovers |
| Network blip disconnects a server | ❌ All messages during outage are lost | ✅ Consumer resumes from last ACK'd position |
| Same event processed twice | ❌ No built-in protection | ✅ Deduplication via SET NX EX |
| Two servers race on same handler | ❌ Both execute simultaneously | ✅ Distributed lock prevents concurrent execution |
| Handler takes too long | ❌ Blocks everything, no timeout | ✅ Per-handler timeout + automatic retry |
| Burst of rapid updates | ❌ Each one triggers a full reload | ✅ Debounce merges rapid events into one execution |

**Ripple is Pub/Sub that actually works in production.**

---

## Prerequisites

| Dependency | Minimum Version | Required Commands |
|------------|----------------|-------------------|
| **Redis** | **5.0+** | `XADD`, `XREADGROUP`, `XACK`, `XGROUP`, `XPENDING`, `XCLAIM` (Streams) |
| Node.js | 16+ | - |
| ioredis | 4+ | - |

Redis Streams were introduced in Redis 5.0. Versions below 5.0 (e.g. the Windows port 3.0.504) will fail at consumer startup with `ERR unknown command 'xgroup'`.

## Architecture

![architecture](docs/architecture.png)

Every server instance creates its own Consumer Group on a shared Redis Stream. When a refresh event is published, all servers independently receive and execute matching handlers — like ripples spreading across water.

## Key Features

| Feature | Mechanism | Description |
|---------|-----------|-------------|
| Broadcast (Fanout) | Consumer Group per server | Every server gets every event |
| At-Least-Once | XPENDING + XCLAIM | Crash recovery for unacknowledged messages |
| Deduplication | SET NX EX | Prevents duplicate execution per (requestId, key, server) |
| Distributed Lock | SET NX PX + Lua CAS | Prevents concurrent execution of same handler |
| Wildcard Matching | minimatch | Glob patterns like `event/*`, `**` |
| Dependency Chains | Topological Sort | Correct execution order across handlers |
| Debounce | In-memory timer | Merges rapid-fire events per key |
| Retry | Exponential backoff | Configurable max retries with delay cap |
| Bootstrap Preload | Parallel execution | Load all data at server startup |
| Metrics | prom-client | Prometheus histograms, counters, gauges |

## Quick Start

```typescript
import { createRipple } from '@gatrix/ripple';

const ripple = createRipple({
  serverId: `lobbyd-${hostname()}`,
  redis: { host: 'redis.internal', port: 6379 },
});

ripple.register({
  key: 'item-table',
  refresh: async (ctx) => {
    // reload item data from database
    const items = await db.query('SELECT * FROM items');
    itemCache.replace(items);
  },
});

await ripple.start();
app.use('/ripple', ripple.createRouter());
```

## Usage Examples

### 1. Basic Registration with Dependencies

```typescript
import { createRipple, Refreshable } from '@gatrix/ripple';

const ripple = createRipple({
  serverId: 'worldd-1',
  redis: { host: 'localhost', port: 6379 },
});

// Base data: no dependencies
ripple.register({
  key: 'item-table',
  refresh: async (ctx) => {
    console.log(`[item-table] trigger=${ctx.trigger}`);
    const rows = await db.query('SELECT * FROM game_items');
    ItemTable.reload(rows);
  },
  timeoutMs: 15000,
});

// Depends on item-table: will always execute AFTER item-table
ripple.register({
  key: 'shop-config',
  refresh: async (ctx) => {
    console.log(`[shop-config] trigger=${ctx.trigger}`);
    const config = await db.query('SELECT * FROM shop_configs');
    ShopManager.reload(config);
  },
  dependsOn: ['item-table'],
  timeoutMs: 10000,
});

// Event data with debounce: rapid edits from data tools are merged
ripple.register({
  key: 'event/summer',
  refresh: async (ctx) => {
    const eventData = await dataService.fetchEvent('summer');
    EventManager.update('summer', eventData);
  },
  debounceMs: 3000,  // 3 second window
});

const result = await ripple.start();
// Bootstrap output:
//   [bootstrap] starting (3 refreshables)
//   [item-table] trigger=bootstrap    -- runs first (no deps)
//   [shop-config] trigger=bootstrap   -- runs after item-table
//   [event/summer] trigger=bootstrap  -- runs in parallel with shop-config
```

## dependsOn vs Pattern Matching

![dependency-vs-wildcard](docs/dependency-vs-wildcard.png)

It is important to understand the clear separation between these two concepts:

### dependsOn = Bootstrap Order Only

`dependsOn` controls **only the execution order during server startup** (bootstrap).
It does NOT trigger automatic cascading refreshes at runtime.

```
Server Startup (bootstrap):

  Layer 1:  [item-table]  [localization/ko]     -- no deps, run first
  Layer 2:  [shop-config] [event/summer]         -- depend on item-table, run after
  Layer 3:  [price-calc]                         -- depends on shop-config, run last
```

When `shop-config` declares `dependsOn: ['item-table']`, it means:
- At **bootstrap**: `item-table` loads before `shop-config`. Guaranteed.
- At **runtime**: refreshing `item-table` does NOT automatically refresh `shop-config`.

This is intentional. Implicit cascading would be:
- Hard to debug ("why did shop-config reload? I only refreshed item-table")
- Hard to predict (deep dependency chains cause unexpected load)
- Redundant (wildcard patterns already cover multi-handler refresh)

### Pattern Matching = Runtime Refresh Scope

At runtime, the **caller explicitly decides** what to refresh using glob patterns:

| Pattern | Effect | Example |
|---------|--------|---------|
| `"item-table"` | Exact match, single handler | Refresh only item data |
| `"event/*"` | All handlers under event/ | Refresh all events |
| `"**"` | All registered handlers | Full data reload |
| `"localization/*"` | All localization data | Refresh all languages |

```bash
# Refresh item-table only (shop-config is NOT affected)
curl -X POST /ripple/refresh -d '{"pattern": "item-table"}'

# Refresh item-table AND shop-config together
curl -X POST /ripple/refresh -d '{"pattern": "{item-table,shop-config}"}'

# Refresh everything after a deployment
curl -X POST /ripple/refresh -d '{"pattern": "**", "triggeredBy": "deploy"}'
```

This design follows the principle: **explicit is better than implicit**.
The caller knows exactly what will be refreshed. No surprises, no hidden cascades.

### 2. Wildcard Refresh via API

```bash
# Refresh all event handlers
curl -X POST http://localhost:3000/ripple/refresh \
  -H "Content-Type: application/json" \
  -d '{"pattern": "event/*", "triggeredBy": "admin-panel"}'

# Response:
# {
#   "requestId": "V1StGXR8_Z5jdHi6B-myT",
#   "pattern": "event/*",
#   "matchedKeys": ["event/summer", "event/halloween", "event/christmas"],
#   "matchedCount": 3,
#   "status": "published"
# }

# Refresh everything
curl -X POST http://localhost:3000/ripple/refresh \
  -d '{"pattern": "**", "triggeredBy": "deployment"}'

# Refresh exact key
curl -X POST http://localhost:3000/ripple/refresh \
  -d '{"pattern": "item-table", "triggeredBy": "data-webhook"}'
```

### 3. Programmatic Publishing (Server-to-Server)

```typescript
// From a data webhook handler:
app.post('/webhook/data-update', async (req, res) => {
  const { contentType } = req.body;

  // Publish refresh event - all servers will pick it up
  const event = RefreshPublisher.createEvent(
    `data/${contentType}`,
    'data-webhook',
  );
  await ripple.publisher.publish(event);

  res.json({ published: true, requestId: event.requestId });
});
```

### 4. Integrating with Existing Logger (e.g., winston, pino)

```typescript
import { createRipple, RippleLoggerFactory } from '@gatrix/ripple';
import myLogger from './my-logger';

// Wrap existing logger to match RippleLoggerFactory interface
const createLogger: RippleLoggerFactory = (module) => ({
  debug: (msg, meta) => myLogger.debug(`[ripple:${module}] ${msg}`, meta),
  info:  (msg, meta) => myLogger.info(`[ripple:${module}] ${msg}`, meta),
  warn:  (msg, meta) => myLogger.warn(`[ripple:${module}] ${msg}`, meta),
  error: (msg, meta) => myLogger.error(`[ripple:${module}] ${msg}`, meta),
});

const ripple = createRipple(config, createLogger);
```

### 5. Graceful Shutdown Integration

```typescript
const ripple = createRipple(config);
await ripple.start();

// Register with your existing shutdown system
process.on('SIGTERM', async () => {
  await ripple.shutdown();
  // Consumer stops, pending debounces flush, Redis disconnects
  process.exit(0);
});
```

### 6. Monitoring with Prometheus

```
GET /ripple/metrics

# HELP ripple_refresh_duration_seconds Duration of refresh handler execution
# TYPE ripple_refresh_duration_seconds histogram
ripple_refresh_duration_seconds_bucket{key="item-table",trigger="refresh",status="success",le="0.1"} 42
ripple_refresh_duration_seconds_bucket{key="item-table",trigger="refresh",status="success",le="0.5"} 47

# HELP ripple_refresh_success_total Total successful refreshes
# TYPE ripple_refresh_success_total counter
ripple_refresh_success_total{key="item-table",trigger="refresh"} 47
ripple_refresh_success_total{key="shop-config",trigger="bootstrap"} 1

# HELP ripple_refresh_running_count Currently running handlers
# TYPE ripple_refresh_running_count gauge
ripple_refresh_running_count{key="item-table"} 0
```

### 7. Debug Mode: Dependency Graph Visualization

When `logLevel` is set to `'debug'`, Ripple prints the dependency graph at startup:

```
Dependency Graph (4 refreshables):
  item-table
    -> shop-config
    -> event/summer
  shop-config [depends on: item-table]
    (no dependents)
  event/summer [depends on: item-table]
    (no dependents)
  localization/ko
    (no dependents)
```

## API Endpoints

### POST /refresh

Publish a refresh event. Returns the list of matched handlers.

**Request:**
```json
{ "pattern": "event/*", "triggeredBy": "admin-api" }
```

**Response 200:**
```json
{
  "requestId": "abc123",
  "pattern": "event/*",
  "matchedKeys": ["event/summer", "event/halloween"],
  "matchedCount": 2,
  "status": "published"
}
```

**Response 404 (no match):**
```json
{ "error": "No refreshables match pattern", "pattern": "unknown/*" }
```

### GET /refreshables

Returns all registered handlers with their configuration.

### GET /metrics

Prometheus text format metrics endpoint.

### GET /health

Simple health check.

## Advanced Settings

All settings below are optional unless marked **[REQUIRED]**. Default values are shown in parentheses.

---

### Core Settings

#### `serverId` **[REQUIRED]**

```typescript
serverId: 'lobbyd-1'
```

Unique identifier for this server instance. Each server MUST have a distinct ID because:
- A dedicated Redis Consumer Group (`group:<serverId>`) is created per server.
- Deduplication keys include serverId to allow each server to process the same event independently.
- Distributed locks are scoped per server.

**Recommended format:** `<processType>-<hostname>` (e.g. `lobbyd-worker-01`, `worldd-us-east-1`)

**WARNING:** If two servers share the same `serverId`, one will steal the other's stream messages. Events will be processed by only one of them, not both.

---

#### `redis` **[REQUIRED]**

```typescript
redis: {
  host: 'redis.internal',   // Redis server hostname
  port: 6379,                // Redis server port
  password: 'secret',        // AUTH password (optional)
  db: 0,                     // SELECT database index (default: 0)
  keyPrefix: 'myapp:',       // Prefix for all Redis keys (optional)
}
```

Ripple creates **two** ioredis connections internally:
1. **Subscriber connection** - dedicated to `XREADGROUP` blocking reads (cannot share with other commands)
2. **Command connection** - for locks, dedup, publish, ACK, and all non-blocking operations

Both connections use the same config. If your Redis requires TLS or other advanced ioredis options, pass them via `redis.options`.

---

#### `logLevel` (default: `'info'`)

```typescript
logLevel: 'info'  // 'debug' | 'info' | 'warn' | 'error' | 'silent'
```

| Level | Output |
|-------|--------|
| `debug` | Everything including dependency graph, dedupe decisions, lock acquire/release |
| `info` | Bootstrap progress, consumer start/stop, refresh results |
| `warn` | Reclaim failures, lock contention, timeout warnings |
| `error` | Handler exceptions, consumer loop crashes |
| `silent` | No output (useful for tests) |

**When to use `debug`:** During initial setup to verify dependency graph and handler registration. Disable in production - debug logs include per-message details that generate high volume.

---

#### `defaultTimeoutMs` (default: `30000`)

```typescript
defaultTimeoutMs: 30000  // 30 seconds
```

Maximum execution time for a single refresh handler. If a handler exceeds this, it is aborted and recorded as `timeout` status. This applies to handlers that do NOT specify their own `timeoutMs`.

**Impact:** Setting too low causes legitimate slow queries to be killed. Setting too high allows a stuck handler to block other refreshes behind a distributed lock.

**Per-handler override:**
```typescript
ripple.register({
  key: 'heavy-analytics',
  timeoutMs: 120000,  // 2 minutes for this specific handler
  refresh: async (ctx) => { /* slow aggregation query */ },
});
```

---

### Stream Settings

Controls Redis Stream behavior for event delivery.

```typescript
stream: {
  key: 'refresh-stream',
  blockMs: 5000,
  batchSize: 10,
  maxLen: 10000,
}
```

#### `stream.key` (default: `'refresh-stream'`)

The Redis key name for the shared stream. All servers in the same cluster MUST use the same key to receive the same events.

**When to change:** If you run multiple independent ripple clusters on the same Redis instance (e.g. staging vs production), use different keys like `'refresh-stream:staging'` and `'refresh-stream:prod'`.

#### `stream.blockMs` (default: `5000`)

Timeout in milliseconds for `XREADGROUP BLOCK`. The consumer waits up to this long for new messages before looping.

| Value | Trade-off |
|-------|-----------|
| `1000` | More responsive (1s max latency), higher Redis CPU from frequent polling |
| `5000` | Good balance for most workloads |
| `30000` | Lower Redis CPU, but up to 30s latency before new events are noticed |

**Impact on shutdown:** Graceful shutdown must wait for the current BLOCK to return. A `blockMs` of 30000 means shutdown could take up to 30 seconds.

#### `stream.batchSize` (default: `10`)

Number of messages to read per `XREADGROUP` call (`COUNT` parameter).

**When to increase:** If your system publishes many events in bursts (e.g. CMS bulk update), increase to 50-100 to process batches efficiently.

**When to decrease:** If each handler is expensive (>5s), set to 1-3 to avoid queuing too many heavy operations.

#### `stream.maxLen` (default: `10000`)

Approximate maximum entries in the stream. Redis uses `MAXLEN ~` (approximate trimming) for performance.

**Impact:** Old entries beyond this limit are deleted. If a server was offline for a long time and the stream has been trimmed past its last-read position, that server will miss those events. It will resume from the current position.

**When to increase:** If servers can be offline for extended periods and you want to retain more history.

**When to decrease:** To reduce Redis memory usage in high-throughput environments.

---

### Retry Settings

Controls automatic retry behavior when a handler fails.

```typescript
retry: {
  maxRetries: 3,
  retryDelayMs: 1000,
  exponentialBackoff: true,
  maxDelayMs: 30000,
}
```

**IMPORTANT:** Retries apply only to **runtime refresh** (trigger=`'refresh'`). Bootstrap failures are NOT retried - they are reported in the bootstrap result.

#### `retry.maxRetries` (default: `3`)

Maximum number of retry attempts after the initial failure. Total executions = 1 (initial) + maxRetries.

| Value | Total attempts | Use case |
|-------|---------------|----------|
| `0` | 1 | No retries, fail immediately (for non-critical data) |
| `3` | 4 | Default, suitable for transient DB/network errors |
| `10` | 11 | For handlers that depend on flaky external APIs |

#### `retry.retryDelayMs` (default: `1000`)

Base delay between retries in milliseconds. With exponential backoff enabled, actual delays are:

```
Attempt 1: 1000ms
Attempt 2: 2000ms
Attempt 3: 4000ms  (capped by maxDelayMs)
```

#### `retry.exponentialBackoff` (default: `true`)

When `true`, each retry waits `retryDelayMs * 2^(attempt-1)`. When `false`, all retries use the same `retryDelayMs`.

**When to disable:** If you want predictable, fixed-interval retries (e.g. polling a service that has a known recovery time).

#### `retry.maxDelayMs` (default: `30000`)

Upper bound for the exponential backoff delay. Prevents delays from growing indefinitely.

**Example with defaults:** Delays would be 1s, 2s, 4s, 8s, 16s, 30s, 30s, 30s... (capped at 30s)

---

### Consumer Settings

Controls the crash recovery mechanism for unacknowledged messages.

```typescript
consumer: {
  pendingReclaimIntervalMs: 30000,
  claimMinIdleMs: 60000,
  claimBatchSize: 100,
}
```

**How it works:** When a server crashes mid-execution, its messages remain in Redis as "pending" (unacknowledged). Other servers (or the same server after restart) periodically scan for these orphaned messages and reclaim them.

#### `consumer.pendingReclaimIntervalMs` (default: `30000`)

How often (in ms) the consumer checks for orphaned pending messages using `XPENDING`.

| Value | Trade-off |
|-------|-----------|
| `5000` | Faster crash recovery, more frequent Redis queries |
| `30000` | Good balance |
| `120000` | Minimal Redis overhead, but orphaned messages wait up to 2 minutes |

#### `consumer.claimMinIdleMs` (default: `60000`)

Minimum time (in ms) a pending message must be idle before it can be reclaimed via `XCLAIM`.

**WARNING:** If this value is too low, a message being legitimately processed by a slow handler could be reclaimed by another server, causing duplicate execution. This value should be **greater than your longest handler timeout + retry delay**.

**Safe formula:** `claimMinIdleMs > defaultTimeoutMs + (maxRetries * maxDelayMs)`

With defaults: `60000 > 30000 + (3 * 30000)` -- This is NOT safe with defaults. Consider increasing to `120000` if you have slow handlers with retries.

#### `consumer.claimBatchSize` (default: `100`)

Maximum number of pending messages to reclaim per cycle.

**When to increase:** After a long server outage with many accumulated pending messages, a higher batch size speeds up recovery.

---

### Deduplication Settings

```typescript
dedupe: {
  ttlSec: 3600,
}
```

#### `dedupe.ttlSec` (default: `3600`)

How long (in seconds) a deduplication key is retained in Redis. During this window, duplicate (requestId + refreshKey + serverId) combinations are silently skipped.

**Impact of too low:** If a message is retried or reclaimed after the TTL expires, it may be executed again (duplicate execution).

**Impact of too high:** More Redis memory usage for dedup keys.

**Recommended:** Keep at default (1 hour). This should exceed the maximum time a message could be pending + retried.

---

### Bootstrap Settings

Controls server startup data loading behavior.

```typescript
bootstrap: {
  parallel: true,
  timeoutMs: 30000,
  failFast: true,
  concurrency: 10,
}
```

#### `bootstrap.parallel` (default: `true`)

When `true`, independent handlers (those not in a dependency chain) run in parallel.

```
parallel=true:   [item-table] + [localization/ko]  (simultaneous)
                 then [shop-config]                 (after item-table)

parallel=false:  [item-table] -> [localization/ko] -> [shop-config]  (one by one)
```

**When to disable:** If your database cannot handle concurrent connections during startup, or if handlers compete for shared resources.

#### `bootstrap.timeoutMs` (default: `30000`)

Total time budget for the entire bootstrap phase. If exceeded, remaining handlers are skipped.

**WARNING:** This is the GLOBAL timeout, not per-handler. If you have 20 handlers each taking 2 seconds, you need at least 40 seconds (sequential) or less with parallelism.

#### `bootstrap.failFast` (default: `true`)

When `true`, bootstrap aborts immediately on the first handler failure. When `false`, it continues and reports all failures at the end.

| Value | Use case |
|-------|----------|
| `true` | Production - critical data must load. If base data fails, there is no point loading dependent data. |
| `false` | Development/testing - see all failures at once to fix them in batch. |

#### `bootstrap.concurrency` (default: `10`)

Maximum number of handlers running simultaneously within a single dependency layer.

**When to decrease:** If bootstrap overloads your database with too many concurrent queries. Set to 3-5 for databases with limited connection pools.

---

### Per-Handler Options

These are set on individual `Refreshable` registrations, not on the global config.

```typescript
ripple.register({
  key: 'event/summer',
  refresh: handler,

  // Per-handler options:
  timeoutMs: 10000,              // Override global defaultTimeoutMs
  dependsOn: ['item-table'],     // Bootstrap ordering (NOT runtime cascade)
  debounceMs: 3000,              // Merge rapid-fire events within this window
});
```

#### `timeoutMs` (per-handler)

Overrides the global `defaultTimeoutMs` for this specific handler.

#### `dependsOn` (per-handler)

Array of handler keys that must complete before this handler during bootstrap. See the [dependsOn vs Pattern Matching](#dependson-vs-pattern-matching) section.

#### `debounceMs` (per-handler)

When set, multiple refresh events for this handler within the debounce window are merged into a single execution. Only the **last** event in the window triggers the actual refresh.

**When to use:** For handlers triggered by CMS edits where an editor might save multiple times in quick succession. A 2-3 second debounce prevents redundant reloads.

**WARNING:** Debounced executions are fire-and-forget. The stream message is ACK'd immediately, and the actual handler runs after the debounce window expires. If the server crashes during the debounce window, the pending execution is lost.

---

## Production Operation Guide

This section covers critical considerations for running ripple in a live service environment.

---

### 1. Redis Infrastructure

#### Dedicated Redis Instance

Ripple should use a **dedicated Redis instance** (or at least a dedicated DB index) separate from your game's primary cache/session Redis. Reasons:

- Stream data accumulates over time and consumes memory
- `XREADGROUP BLOCK` holds a connection open, which counts against `maxclients`
- Avoid interference with game-critical cache operations

```typescript
const ripple = createRipple({
  serverId: 'lobbyd-1',
  redis: {
    host: 'redis-ripple.internal',  // Dedicated instance
    port: 6379,
    db: 0,
  },
});
```

#### Redis Persistence

If Redis restarts without persistence (`RDB` or `AOF`), **all Stream data and consumer group state is lost**. This means:

- Pending (unprocessed) messages are permanently lost
- Consumer groups must be recreated (ripple handles this automatically)
- No re-delivery of events published during downtime

**Recommendation:** Enable at least `RDB` snapshots on the ripple Redis instance. `AOF` with `everysec` provides stronger durability at a small performance cost.

#### Redis Cluster / Sentinel

Ripple currently uses a single Redis connection. If you need high availability:

- **Redis Sentinel:** ioredis supports Sentinel natively. Pass Sentinel config via `redis.options`.
- **Redis Cluster:** Not recommended. Streams and consumer groups operate on a single key, so there is no benefit from sharding. Cluster adds complexity without value for this use case.

```typescript
// Sentinel example
const ripple = createRipple({
  serverId: 'lobbyd-1',
  redis: {
    host: 'sentinel-host',
    port: 26379,
    options: {
      sentinels: [
        { host: 'sentinel-1', port: 26379 },
        { host: 'sentinel-2', port: 26379 },
        { host: 'sentinel-3', port: 26379 },
      ],
      name: 'ripple-master',
    },
  },
});
```

---

### 2. Stream Memory Management

Redis Streams grow indefinitely unless trimmed. In a live service, this **will** eventually cause OOM.

#### Automatic Trimming (Recommended)

Configure `stream.maxLen` to cap the stream size:

```typescript
const ripple = createRipple({
  // ...
  stream: {
    key: 'ripple:refresh',
    maxLen: 10000,  // Keep last 10,000 entries (~approximate)
  },
});
```

With `maxLen` set, every `XADD` includes `MAXLEN ~10000`, which trims old entries approximately. Redis uses `~` (approximate) trimming internally for performance.

#### Sizing `maxLen`

Calculate based on your event rate:

```
maxLen = (events per second) * (maximum acceptable replay window in seconds)

Example:
  - 2 events/second average, 10 events/second peak
  - Want 1 hour replay window for crash recovery
  - maxLen = 10 * 3600 = 36000
```

If `maxLen` is too small, crash-recovered servers may miss events that were already trimmed. This is acceptable if bootstrap re-initializes all data anyway.

#### Monitoring Stream Size

```bash
# Check stream length and memory usage
redis-cli XLEN ripple:refresh
redis-cli MEMORY USAGE ripple:refresh
redis-cli XINFO STREAM ripple:refresh
```

---

### 3. Consumer Group Lifecycle

#### Orphaned Consumer Groups

When a server instance is permanently decommissioned (removed, not just restarted), its consumer group remains in Redis. These orphaned groups:

- Prevent Redis from trimming old entries (if they have pending messages)
- Waste memory and clutter `XINFO GROUPS` output

**Cleanup procedure:**

```bash
# List all consumer groups
redis-cli XINFO GROUPS ripple:refresh

# Delete orphaned group (server no longer exists)
redis-cli XGROUP DESTROY ripple:refresh "group:lobbyd-old-instance"
```

**Recommendation:** Run a periodic cleanup job (e.g., weekly cron) that compares active server IDs against consumer groups and removes stale ones.

#### Server Restart Behavior

When a server restarts with the same `serverId`:

1. The existing consumer group is reused (not recreated)
2. Any pending (unACK'd) messages from the previous run are reprocessed
3. This provides **at-least-once** delivery guarantee across restarts

This is by design. Ensure your handlers are **idempotent** (see below).

---

### 4. Handler Implementation Rules

#### Handlers MUST Be Idempotent

Ripple guarantees **at-least-once** delivery. The same event may be processed more than once due to:

- Server crash before ACK
- Network partition with Redis
- `XCLAIM` recovering pending messages

**Bad (not idempotent):**
```typescript
refresh: async (ctx) => {
  // Appends to a list every time - duplicates cause data growth
  const items = await db.query('SELECT * FROM items');
  existingItems.push(...items);
}
```

**Good (idempotent):**
```typescript
refresh: async (ctx) => {
  // Full replacement - safe to execute multiple times
  const items = await db.query('SELECT * FROM items');
  itemCache.replaceAll(items);
}
```

#### Handlers MUST NOT Block Indefinitely

Every handler has a timeout (`defaultTimeoutMs` or per-handler `timeoutMs`). If a handler exceeds this timeout:

- It is **killed** (the Promise race is lost)
- The result is reported as `timeout`
- But the underlying async operation (DB query, HTTP call) may still be running

**Critical:** Ensure your data sources (DB, HTTP APIs) have their own timeouts set **lower** than ripple's handler timeout:

```typescript
{
  key: 'item-table',
  timeoutMs: 10000,  // Ripple timeout: 10s
  refresh: async (ctx) => {
    // DB query timeout should be < 10s
    const items = await db.query('SELECT * FROM items', { timeout: 8000 });
    itemCache.replaceAll(items);
  },
}
```

#### Handlers Should Not Throw Expected Errors

Throwing inside a handler triggers the retry mechanism. If the error is a known, non-transient condition (e.g., "table does not exist"), retrying is wasteful. Catch and log these internally:

```typescript
refresh: async (ctx) => {
  try {
    const data = await loadData();
    cache.replaceAll(data);
  } catch (err) {
    if (isNonTransient(err)) {
      logger.error('Non-transient error, skipping retry', err);
      return; // Do not throw - prevents retry
    }
    throw err; // Transient error - allow retry
  }
}
```

---

### 5. Network and Reconnection

#### Redis Connection Loss

ioredis automatically reconnects when the Redis connection drops. During reconnection:

- The consumer's `XREADGROUP BLOCK` call fails
- The consumer loop retries on the next poll cycle
- Published events are queued by ioredis and sent after reconnection

No events are lost during brief network interruptions, but **extended outages** (longer than `claimMinIdleMs`) may cause other servers to claim this server's pending messages.

#### Cross-Datacenter Deployment

If game servers span multiple datacenters but share one Redis instance:

- Network latency affects `XREADGROUP BLOCK` responsiveness
- Consider deploying a Redis instance per datacenter with separate ripple streams
- Or use Redis replication with a single primary

---

### 6. Monitoring and Alerting

#### Key Metrics to Monitor

Ripple exposes Prometheus metrics via `prom-client`. Essential alerts:

| Metric | Alert Condition | Meaning |
|--------|----------------|---------|
| `ripple_refresh_total{status="failure"}` | Rate > 0 sustained | Handlers are failing after all retries |
| `ripple_refresh_total{status="timeout"}` | Rate > 0 sustained | Handlers are too slow |
| `ripple_running_count` | Value stuck > 0 for minutes | Handler may be hanging |
| `ripple_publish_total` | Sudden drop to 0 | Publisher may be disconnected |
| Stream lag (XPENDING count) | Growing over time | Consumer falling behind |

#### Checking Consumer Health

```bash
# How many unprocessed messages per consumer group?
redis-cli XPENDING ripple:refresh "group:lobbyd-1" - + 10

# How far behind is each consumer?
redis-cli XINFO GROUPS ripple:refresh
# Check "lag" field (Redis 7.0+)
```

#### Log Patterns to Watch

| Log Message | Severity | Action |
|-------------|----------|--------|
| `Refresh exhausted all retries` | ERROR | Handler is broken - investigate root cause |
| `Refresh timed out` | ERROR | Handler too slow - increase timeout or optimize |
| `Refresh completed but slow` | WARN | Handler approaching timeout - optimize proactively |
| `Skipped: lock already held` | WARN | Concurrent execution detected - normal in multi-publish scenarios |

---

### 7. Scaling Considerations

#### Number of Handlers

There is no hard limit on registered handlers, but consider:

- Bootstrap runs handlers in dependency-ordered layers with concurrency limits (`bootstrap.concurrency`, default 10)
- Each refresh event triggers pattern matching against ALL registered handlers
- More handlers = longer bootstrap time

**Recommendation:** Keep handlers at a manageable count (< 50). Group related data into single handlers rather than creating one per table.

#### Event Publish Rate

Ripple processes events sequentially per consumer (one `XREADGROUP` poll at a time, `batchSize` entries per poll). Under high publish rates:

- Increase `consumer.batchSize` to process more events per poll cycle
- Use `debounceMs` on frequently-triggered handlers to coalesce rapid events
- Monitor XPENDING count to detect consumer falling behind

#### Multiple Server Types

If different server types (lobby, game, match) need different refresh sets, use separate stream keys:

```typescript
// Lobby servers
createRipple({ stream: { key: 'ripple:lobby' }, ... });

// Game servers
createRipple({ stream: { key: 'ripple:game' }, ... });
```

This prevents game servers from receiving lobby-only events and vice versa.

---

### 8. Common Pitfalls

| Pitfall | Symptom | Solution |
|---------|---------|----------|
| Redis < 5.0 | `ERR unknown command 'xgroup'` at startup | Upgrade Redis to 5.0+ |
| Same `serverId` across instances | Only one instance processes events | Use unique `serverId` per process (e.g., hostname + PID) |
| Handler modifies shared mutable state without synchronization | Race conditions, corrupted cache | Use full-replacement pattern, not incremental updates |
| `maxLen` not set | Redis OOM after weeks/months | Set `stream.maxLen` to a reasonable value |
| `defaultTimeoutMs` too low | Handlers killed during normal operation | Set timeout to 2-3x the expected handler duration |
| `defaultTimeoutMs` too high | Slow handlers hold distributed locks for too long | Keep timeout under 60s; optimize slow handlers |
| `claimMinIdleMs` < actual handler execution time | Messages claimed while still being processed | Set `claimMinIdleMs` > `defaultTimeoutMs` + `maxRetries * maxDelayMs` |
| Non-idempotent handlers | Duplicate data on retry/crash recovery | Always use full-replacement, never incremental append |
| Bootstrap `failFast: true` in production | One broken handler prevents all servers from starting | Use `failFast: false` in production, `true` only in development |
| Orphaned consumer groups | Stream memory grows indefinitely | Periodically clean up groups for decommissioned servers |

---

## Deploy to Game Server

```bash
cd packages/ripple
yarn deploy:game         # build -> pack -> copy to game/server/node/lib/
yarn deploy:game --bump  # bump patch version + deploy
```

After deploying, in the game server:
```bash
cd game/server/node
yarn install
yarn build
```

Then import in game server code:
```typescript
import { createRipple } from '@gatrix/ripple';
```

## License

MIT
