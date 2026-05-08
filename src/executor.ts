// ---------------------------------------------------------------------------
// @gatrix/ripple ??Refresh Executor
// ---------------------------------------------------------------------------

import { nanoid } from 'nanoid';
import { RippleLogger, RippleLoggerFactory } from './logger';
import {
  Refreshable,
  RefreshContext,
  RefreshResult,
  RefreshTrigger,
  RetryConfig,
  DEFAULT_RETRY_CONFIG,
} from './types';
import { DistributedLock } from './lock';
import { RippleMetrics } from './metrics';

/**
 * Executes a single refreshable with:
 * - Distributed lock acquisition
 * - Timeout enforcement
 * - Retry with exponential backoff
 * - Metrics recording
 */
export class RefreshExecutor {
  private readonly lock: DistributedLock;
  private readonly metrics: RippleMetrics;
  private readonly logger: RippleLogger;
  private readonly retryConfig: RetryConfig;
  private readonly serverId: string;
  private readonly defaultTimeoutMs: number;

  constructor(opts: {
    lock: DistributedLock;
    metrics: RippleMetrics;
    createLogger: RippleLoggerFactory;
    serverId: string;
    retryConfig?: Partial<RetryConfig>;
    defaultTimeoutMs?: number;
  }) {
    this.lock = opts.lock;
    this.metrics = opts.metrics;
    this.logger = opts.createLogger('executor');
    this.serverId = opts.serverId;
    this.defaultTimeoutMs = opts.defaultTimeoutMs ?? 30000;
    this.retryConfig = { ...DEFAULT_RETRY_CONFIG, ...opts.retryConfig };
  }

  /**
   * Execute a refreshable handler with full protection:
   * lock ??timeout ??execute ??retry on failure ??metrics.
   */
  async execute(
    refreshable: Refreshable,
    ctx: RefreshContext,
  ): Promise<RefreshResult> {
    const startTime = Date.now();
    const timeoutMs = refreshable.timeoutMs ?? this.defaultTimeoutMs;
    const lockId = nanoid();
    const log = this.logger;
    const logCtx = {
      refreshKey: refreshable.key,
      requestId: ctx.requestId,
      trigger: ctx.trigger,
    };

    // Acquire lock
    const acquired = await this.lock.acquire(
      this.serverId,
      refreshable.key,
      timeoutMs + 5000, // lock TTL = timeout + buffer
      lockId,
    );

    if (!acquired) {
      log.warn('Skipped: lock already held (concurrent execution)', logCtx);
      return {
        key: refreshable.key,
        status: 'skipped',
        durationMs: Date.now() - startTime,
      };
    }

    this.metrics.runningCount.labels(refreshable.key).inc();

    try {
      const result = await this.executeWithRetry(
        refreshable,
        ctx,
        timeoutMs,
        log,
        logCtx,
      );

      const durationMs = Date.now() - startTime;
      const durationSec = durationMs / 1000;

      this.metrics.recordExecution(
        refreshable.key,
        ctx.trigger,
        result.status,
        durationSec,
      );

      return { ...result, durationMs };
    } finally {
      this.metrics.runningCount.labels(refreshable.key).dec();
      await this.lock.release(this.serverId, refreshable.key, lockId);
    }
  }

  private async executeWithRetry(
    refreshable: Refreshable,
    originalCtx: RefreshContext,
    timeoutMs: number,
    log: RippleLogger,
    logCtx: Record<string, unknown>,
  ): Promise<RefreshResult> {
    const effectiveRetryConfig = { ...this.retryConfig, ...refreshable.retry };
    let lastError: Error | undefined;

    const maxAttempts =
      originalCtx.trigger === 'bootstrap'
        ? 1 // bootstrap: no retry, fail-fast
        : effectiveRetryConfig.maxRetries + 1;

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      const isRetry = attempt > 0;
      const ctx: RefreshContext = isRetry
        ? {
            ...originalCtx,
            trigger: 'retry' as RefreshTrigger,
            retryCount: attempt,
            startedAt: Date.now(),
          }
        : originalCtx;

      try {
        await this.executeWithTimeout(refreshable, ctx, timeoutMs);

        const elapsed = Date.now() - ctx.startedAt;
        const logFields: Record<string, unknown> = {
          durationMs: elapsed,
          attempt,
          status: 'success',
        };

        // Warn if handler is slow (>50% of timeout)
        if (elapsed > timeoutMs * 0.5) {
          log.warn('Refresh completed but slow (exceeds 50% of timeout)', {
            ...logCtx,
            ...logFields,
            timeoutMs,
            usagePercent: Math.round((elapsed / timeoutMs) * 100),
          });
        } else {
          log.info('Refresh completed', { ...logCtx, ...logFields });
        }

        return { key: refreshable.key, status: 'success', durationMs: 0 };
      } catch (err) {
        lastError = err instanceof Error ? err : new Error(String(err));
        const isTimeout = lastError.message === 'REFRESH_TIMEOUT';
        const elapsed = Date.now() - ctx.startedAt;

        if (isTimeout) {
          log.error('Refresh timed out', {
            ...logCtx,
            attempt,
            timeoutMs,
            elapsedMs: elapsed,
          });
          return {
            key: refreshable.key,
            status: 'timeout',
            durationMs: elapsed,
            error: `Timeout after ${timeoutMs}ms (elapsed: ${elapsed}ms)`,
          };
        }

        log.warn('Refresh failed', {
          ...logCtx,
          attempt,
          elapsedMs: elapsed,
          error: lastError.message,
          stack: lastError.stack,
          maxAttempts,
        });

        // Wait before retry (skip for last attempt)
        if (attempt < maxAttempts - 1) {
          const delay = this.calculateDelay(attempt);
          log.debug('Retrying after delay', {
            ...logCtx,
            delay,
            nextAttempt: attempt + 1,
          });
          await this.sleep(delay);
        }
      }
    }

    log.error('Refresh exhausted all retries', {
      ...logCtx,
      error: lastError?.message,
      stack: lastError?.stack,
      attempts: maxAttempts,
    });

    return {
      key: refreshable.key,
      status: 'failure',
      durationMs: 0,
      error: lastError?.message ?? 'Unknown error',
      retryCount: maxAttempts - 1,
    };
  }

  private async executeWithTimeout(
    refreshable: Refreshable,
    ctx: RefreshContext,
    timeoutMs: number,
  ): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error('REFRESH_TIMEOUT'));
      }, timeoutMs);

      refreshable
        .refresh(ctx)
        .then(() => {
          clearTimeout(timer);
          resolve();
        })
        .catch((err) => {
          clearTimeout(timer);
          reject(err);
        });
    });
  }

  private calculateDelay(attempt: number): number {
    if (this.retryConfig.exponentialBackoff) {
      const delay = this.retryConfig.retryDelayMs * Math.pow(2, attempt);
      return Math.min(delay, this.retryConfig.maxDelayMs);
    }
    return this.retryConfig.retryDelayMs;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
