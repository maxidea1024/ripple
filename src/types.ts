// ---------------------------------------------------------------------------
// @gatrix/ripple — Type Definitions
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Core Types
// ---------------------------------------------------------------------------

/**
 * Trigger source that caused this refresh invocation.
 *
 * - `bootstrap`: server startup preload
 * - `refresh`:   runtime refresh via Pub/Sub event
 * - `retry`:     automatic retry after a failed attempt
 */
export type RefreshTrigger = 'bootstrap' | 'refresh' | 'retry';

/**
 * Context passed to every refresh handler.
 * Handlers can branch logic based on trigger type.
 */
export interface RefreshContext {
  /** Why this refresh was invoked */
  trigger: RefreshTrigger;

  /** Unique identifier for the refresh request (absent during bootstrap) */
  requestId?: string;

  /** Glob pattern that caused this handler to be selected */
  pattern?: string;

  /** Current retry attempt (0-based, only present when trigger === 'retry') */
  retryCount?: number;

  /** Epoch ms when this refresh execution started */
  startedAt: number;

  /** Optional key-value metadata passed from the publisher */
  metadata?: Record<string, string>;
}

/**
 * A refreshable unit of data.
 * Register instances with the orchestrator to participate in the refresh cycle.
 */
export interface Refreshable {
  /** Unique hierarchical key, e.g. 'event/summer', 'localization/ko' */
  key: string;

  /** The refresh handler */
  refresh(ctx: RefreshContext): Promise<void>;

  /** Per-handler timeout in ms (default: from global config) */
  timeoutMs?: number;

  /** Keys that must be refreshed before this one */
  dependsOn?: string[];

  /** Debounce window in ms; repeated triggers within this window are merged */
  debounceMs?: number;

  /** Per-handler retry override. Set { maxRetries: 0 } to disable retries. */
  retry?: Partial<RetryConfig>;
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

/** Payload published via Redis Pub/Sub. */
export interface RefreshEvent {
  /** Unique request identifier */
  requestId: string;

  /** Glob pattern (e.g. 'cms/*') */
  pattern: string;

  /** Environment identifier for per-environment scoping */
  environmentId: string;

  /** Optional origin identifier (e.g. 'admin-api', 'uwocli') */
  triggeredBy?: string;

  /**
   * If true, handlers that depend on matched handlers (via `dependsOn`)
   * will also be refreshed automatically, in topological order.
   * Default: false
   */
  cascade?: boolean;

  /** Epoch ms when the event was created */
  createdAt: number;

  /** Optional key-value metadata (e.g. { tableName: 'Item' }) */
  metadata?: Record<string, string>;
}

// ---------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------

export type RefreshStatus = 'success' | 'failure' | 'timeout' | 'skipped';

/** Result of a single refreshable execution. */
export interface RefreshResult {
  key: string;
  status: RefreshStatus;
  durationMs: number;
  error?: string;
  retryCount?: number;
}

/** Report sent via onExecutionComplete callback after each handler execution. */
export interface ExecutionReport {
  requestId: string;
  environmentId: string;
  serverId: string;
  serviceType: string;
  handlerKey: string;
  status: RefreshStatus;
  durationMs: number;
  error?: string;
}

/** Result of bootstrap loading. */
export interface BootstrapResult {
  totalCount: number;
  successCount: number;
  failureCount: number;
  durationMs: number;
  results: RefreshResult[];
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

export interface RedisConfig {
  host: string;
  port: number;
  password?: string;
  db?: number;
  keyPrefix?: string;
  /** Additional ioredis options */
  options?: Record<string, unknown>;
}

export interface RetryConfig {
  /** Maximum retry attempts (default: 3) */
  maxRetries: number;
  /** Base delay between retries in ms (default: 1000) */
  retryDelayMs: number;
  /** Use exponential backoff (default: true) */
  exponentialBackoff: boolean;
  /** Maximum delay cap in ms (default: 30000) */
  maxDelayMs: number;
}

export interface DedupeConfig {
  /** TTL for dedup keys in seconds (default: 300) */
  ttlSec: number;
}

export interface BootstrapOptions {
  /** Run refreshables in parallel (default: true) */
  parallel: boolean;
  /** Global timeout for entire bootstrap in ms (default: 30000) */
  timeoutMs: number;
  /** Abort on first failure (default: true) */
  failFast: boolean;
  /** Max concurrent refreshables when parallel (default: 10) */
  concurrency: number;
}

export interface ApiConfig {
  /** Whether to enable the built-in Express router (default: true) */
  enabled: boolean;
}

export interface PubSubConfig {
  /** Pub/Sub channel name (default: 'ripple:fanout') */
  channel: string;
}

export interface OrchestratorConfig {
  /** Unique server identifier (default: hostname-pid) */
  serverId?: string;

  /** Service type identifier (e.g. 'lobbyd', 'authd', 'admind') */
  serviceType: string;

  /**
   * Environment identifier for per-environment channel scoping.
   * Events are published/subscribed on channel `ripple:fanout:${environmentId}`.
   */
  environmentId: string;

  redis: RedisConfig;
  pubsub?: Partial<PubSubConfig>;
  retry?: Partial<RetryConfig>;
  dedupe?: Partial<DedupeConfig>;
  bootstrap?: Partial<BootstrapOptions>;
  api?: Partial<ApiConfig>;

  /** Default per-handler timeout in ms (default: 30000) */
  defaultTimeoutMs?: number;

  /** Log level for pino (default: 'info') */
  logLevel?: string;

  /**
   * Callback invoked after each handler execution completes.
   * Use this to record per-server execution results to a database.
   * Called fire-and-forget — errors are logged but do not affect processing.
   */
  onExecutionComplete?: (report: ExecutionReport) => Promise<void> | void;

  /**
   * Callback invoked after a refresh event is published via the API router.
   * Use this to record the refresh request to a history database (e.g. c_ripple_history).
   * Called fire-and-forget — errors do not affect the API response.
   */
  onRefreshPublished?: (info: {
    requestId: string;
    pattern: string;
    environmentId: string;
    triggeredBy: string;
    cascade: boolean;
    receiverCount: number;
    matchedKeys: string[];
    metadata?: Record<string, string>;
  }) => void;
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

export const DEFAULT_PUBSUB_CONFIG: PubSubConfig = {
  channel: 'ripple:fanout',
};

export const DEFAULT_RETRY_CONFIG: RetryConfig = {
  maxRetries: 3,
  retryDelayMs: 1000,
  exponentialBackoff: true,
  maxDelayMs: 30000,
};

export const DEFAULT_DEDUPE_CONFIG: DedupeConfig = {
  ttlSec: 300,
};

export const DEFAULT_BOOTSTRAP_OPTIONS: BootstrapOptions = {
  parallel: true,
  timeoutMs: 30000,
  failFast: true,
  concurrency: 10,
};
