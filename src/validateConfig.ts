// ---------------------------------------------------------------------------
// @gatrix/ripple — Configuration Validation
//
// Validates all Ripple configuration values at startup to prevent runtime
// failures caused by misconfiguration. Runs before any I/O operations.
// ---------------------------------------------------------------------------

import {
  OrchestratorConfig,
  DEFAULT_RETRY_CONFIG,
} from './types';
import { RippleLogger } from './logger';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface ValidationIssue {
  field: string;
  message: string;
  resolution: string;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Validate all Ripple configuration values.
 *
 * - Errors: throw immediately with a summary of ALL issues.
 * - Warnings: log but allow startup to continue.
 *
 * Call this before connecting to Redis to catch config issues early.
 */
export function validateConfig(
  config: OrchestratorConfig,
  registeredCount: number,
  log: RippleLogger,
): void {
  const errors: ValidationIssue[] = [];
  const warnings: ValidationIssue[] = [];

  // ── Redis ──
  if (!config.redis?.host) {
    errors.push({
      field: 'redis.host',
      message: 'Redis host is required',
      resolution: 'Set redis.host in Ripple config (e.g. "localhost")',
    });
  }

  const port = config.redis?.port;
  if (port !== undefined && (port < 1 || port > 65535 || !Number.isInteger(port))) {
    errors.push({
      field: 'redis.port',
      message: `Invalid port: ${port} (must be integer 1-65535)`,
      resolution: 'Set redis.port to a valid port number (e.g. 6379)',
    });
  }

  // ── environmentId ──
  if (!config.environmentId || typeof config.environmentId !== 'string' || config.environmentId.trim().length === 0) {
    errors.push({
      field: 'environmentId',
      message: 'environmentId is required and must be a non-empty string',
      resolution: 'Set environmentId to identify this environment (e.g. "prod-kr", "staging")',
    });
  }

  // ── serverId ──
  const serverId = config.serverId;
  if (serverId !== undefined && (typeof serverId !== 'string' || serverId.trim().length === 0)) {
    errors.push({
      field: 'serverId',
      message: 'serverId must be a non-empty string',
      resolution: 'Set serverId to a unique identifier (e.g. hostname + PID)',
    });
  }

  // ── Timeout ──
  const defaultTimeoutMs = config.defaultTimeoutMs ?? 30000;

  if (defaultTimeoutMs < 1000) {
    errors.push({
      field: 'defaultTimeoutMs',
      message: `defaultTimeoutMs is ${defaultTimeoutMs}ms — handlers will be force-killed before they can complete`,
      resolution: 'Set defaultTimeoutMs to at least 1000ms (recommended: 5000-30000ms)',
    });
  }

  if (defaultTimeoutMs > 120000) {
    warnings.push({
      field: 'defaultTimeoutMs',
      message: `defaultTimeoutMs is ${defaultTimeoutMs}ms (>120s) — slow handlers will block distributed locks for too long`,
      resolution: 'Keep defaultTimeoutMs under 120000ms. If a handler needs more time, set per-handler timeoutMs instead',
    });
  }

  // ── Retry config ──
  const retry = { ...DEFAULT_RETRY_CONFIG, ...config.retry };

  if (retry.maxRetries < 0) {
    errors.push({
      field: 'retry.maxRetries',
      message: `maxRetries is ${retry.maxRetries} (must be >= 0)`,
      resolution: 'Set retry.maxRetries to 0 (no retries) or a positive integer',
    });
  }

  if (retry.retryDelayMs < 100) {
    warnings.push({
      field: 'retry.retryDelayMs',
      message: `retryDelayMs is ${retry.retryDelayMs}ms — very aggressive retries may overwhelm the system`,
      resolution: 'Set retry.retryDelayMs to at least 100ms (recommended: 1000ms)',
    });
  }

  // ── Bootstrap ──
  const bootstrap = config.bootstrap;
  if (bootstrap?.failFast === true) {
    warnings.push({
      field: 'bootstrap.failFast',
      message: 'failFast is true — one handler failure will block server startup entirely',
      resolution: 'In production, set bootstrap.failFast to false to allow partial startup',
    });
  }

  // ── Handler count ──
  if (registeredCount === 0) {
    warnings.push({
      field: '(handlers)',
      message: 'No refresh handlers registered — Ripple will start but do nothing',
      resolution: 'Register at least one handler via ripple.register() before start()',
    });
  }

  // ── Emit warnings ──
  for (const w of warnings) {
    log.warn(`[config] ${w.field}: ${w.message}`, { resolution: w.resolution });
  }

  // ── Throw on errors ──
  if (errors.length > 0) {
    const lines = errors.map(
      (e, i) => `  ${i + 1}. [${e.field}] ${e.message}\n     → Fix: ${e.resolution}`
    );
    throw new Error(
      `[ripple] Configuration validation failed (${errors.length} error(s)):\n` +
      lines.join('\n') + '\n\n' +
      'Fix the above configuration issues before starting Ripple.'
    );
  }

  log.info('Configuration validated', { registeredCount, environmentId: config.environmentId });
}

// ---------------------------------------------------------------------------
// Runtime checks (require Redis connection)
// ---------------------------------------------------------------------------

/**
 * Verify serverId uniqueness using a Redis key with TTL.
 * If another instance with the same serverId is detected, logs a warning.
 */
export async function verifyServerIdUniqueness(
  redis: any,
  serverId: string,
  log: RippleLogger,
): Promise<void> {
  const key = `ripple:instance:${serverId}`;
  try {
    const existing = await redis.get(key);
    if (existing) {
      log.warn(
        `Another Ripple instance with serverId '${serverId}' was active at ${existing}. ` +
        `If that instance is still running, ONLY ONE will process events (the other will be idle).`,
        {
          serverId,
          lastSeen: existing,
          resolution: 'Ensure each process has a unique serverId (e.g. hostname + PID). ' +
            'If the other instance was stopped, this warning will clear after 60 seconds.',
        },
      );
    }
    // Register self with TTL
    await redis.set(key, new Date().toISOString(), 'EX', 60);
  } catch (err) {
    // Non-fatal: if this fails, we still start
    log.debug('serverId uniqueness check failed (non-fatal)', { error: (err as Error).message });
  }
}

/**
 * Renew the serverId heartbeat. Call periodically (e.g. every 30s).
 */
export async function renewServerIdHeartbeat(
  redis: any,
  serverId: string,
): Promise<void> {
  try {
    await redis.set(`ripple:instance:${serverId}`, new Date().toISOString(), 'EX', 60);
  } catch {
    // Silently ignore heartbeat failures
  }
}
