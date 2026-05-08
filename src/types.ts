// ---------------------------------------------------------------------------
// @gatrix/ripple ??Type Definitions
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Core Types
// ---------------------------------------------------------------------------

/**
 * Trigger source that caused this refresh invocation.
 *
 * - `bootstrap`: server startup preload
 * - `refresh`:   runtime refresh via Redis Stream event
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

/** Payload published to the Redis Stream. */
export interface RefreshEvent {
  /** Unique request identifier */
  requestId: string;

  /** Glob pattern (e.g. 'event/*') */
  pattern: string;

  /** Optional origin identifier (e.g. 'admin-api', 'data-webhook') */
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
// Results & History
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

/** Represents a single execution history event */
export interface RippleHistoryEvent {
  eventId: string;
  serverId: string;
  serviceType?: string;
  requestId: string;
  pattern: string;
  handlerKey: string;
  status: RefreshStatus;
  durationMs: number;
  delayMs: number;
  error?: string;
  retryCount?: number;
  triggeredBy?: string;
  createdAt: number;
  startedAt: number;
  finishedAt: number;
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

export interface StreamConfig {
  /** Redis Stream key name (default: 'refresh-stream') */
  key: string;
  /** XREADGROUP BLOCK timeout in ms (default: 5000) */
  blockMs: number;
  /** XREADGROUP COUNT (default: 10) */
  batchSize: number;
  /** MAXLEN for stream trimming (default: 10000) */
  maxLen: number;
}

export interface ConsumerConfig {
  /** Interval for pending message reclaim in ms (default: 30000) */
  pendingReclaimIntervalMs: number;
  /** Minimum idle time for XCLAIM in ms (default: 60000) */
  claimMinIdleMs: number;
  /** Maximum number of pending messages to reclaim per cycle (default: 100) */
  claimBatchSize: number;
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
  /** TTL for dedup keys in seconds (default: 3600) */
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

export interface HistoryConfig {
  /** Redis Stream key name for history (default: 'ripple:history') */
  key: string;
  /** MAXLEN for history stream trimming (default: 5000) */
  maxLen: number;
}

export interface OrchestratorConfig {
  /** Unique server identifier (default: hostname-pid) */
  serverId?: string;

  /** Service type identifier (e.g. 'lobbyd', 'authd', 'admind') */
  serviceType: string;

  redis: RedisConfig;
  stream?: Partial<StreamConfig>;
  consumer?: Partial<ConsumerConfig>;
  retry?: Partial<RetryConfig>;
  dedupe?: Partial<DedupeConfig>;
  bootstrap?: Partial<BootstrapOptions>;
  api?: Partial<ApiConfig>;
  history?: Partial<HistoryConfig>;

  /** Default per-handler timeout in ms (default: 30000) */
  defaultTimeoutMs?: number;

  /** Log level for pino (default: 'info') */
  logLevel?: string;
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

export const DEFAULT_HISTORY_CONFIG: HistoryConfig = {
  key: 'ripple:history',
  maxLen: 5000,
};

export const DEFAULT_STREAM_CONFIG: StreamConfig = {
  key: 'refresh-stream',
  blockMs: 5000,
  batchSize: 10,
  maxLen: 10000,
};

export const DEFAULT_CONSUMER_CONFIG: ConsumerConfig = {
  pendingReclaimIntervalMs: 30000,
  claimMinIdleMs: 60000,
  claimBatchSize: 100,
};

export const DEFAULT_RETRY_CONFIG: RetryConfig = {
  maxRetries: 3,
  retryDelayMs: 1000,
  exponentialBackoff: true,
  maxDelayMs: 30000,
};

export const DEFAULT_DEDUPE_CONFIG: DedupeConfig = {
  ttlSec: 3600,
};

export const DEFAULT_BOOTSTRAP_OPTIONS: BootstrapOptions = {
  parallel: true,
  timeoutMs: 30000,
  failFast: true,
  concurrency: 10,
};
