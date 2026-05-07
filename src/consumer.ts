// ---------------------------------------------------------------------------
// @gatrix/ripple ??Stream Consumer
// ---------------------------------------------------------------------------

import Redis from 'ioredis';
import { RippleLogger, RippleLoggerFactory } from './logger';
import {
  RefreshEvent,
  RefreshContext,
  StreamConfig,
  ConsumerConfig,
  DedupeConfig,
  DEFAULT_STREAM_CONFIG,
  DEFAULT_CONSUMER_CONFIG,
  DEFAULT_DEDUPE_CONFIG,
} from './types';
import { RefreshableRegistry } from './registry';
import { RefreshExecutor } from './executor';
import { DedupeChecker } from './dedupe';
import { DebounceManager } from './debounce';
import { RippleMetrics } from './metrics';

/**
 * Consumes refresh events from the Redis Stream.
 *
 * - Creates a per-server consumer group (broadcast/fanout).
 * - Processes events: dedupe ??wildcard match ??debounce ??execute ??ACK.
 * - Periodically reclaims pending (unacknowledged) messages.
 */
export class StreamConsumer {
  private readonly redis: Redis.Redis;
  private readonly logger: RippleLogger;
  private readonly registry: RefreshableRegistry;
  private readonly executor: RefreshExecutor;
  private readonly dedupe: DedupeChecker;
  private readonly debounce: DebounceManager;
  private readonly metrics: RippleMetrics;
  private readonly serverId: string;
  private readonly streamConfig: StreamConfig;
  private readonly consumerConfig: ConsumerConfig;
  private readonly dedupeConfig: DedupeConfig;

  private running = false;
  private inflightCount = 0;
  private reclaimTimer: ReturnType<typeof setInterval> | null = null;
  private groupName: string;
  private consumerName = 'worker';

  constructor(opts: {
    redis: Redis.Redis;
    createLogger: RippleLoggerFactory;
    registry: RefreshableRegistry;
    executor: RefreshExecutor;
    dedupe: DedupeChecker;
    debounce: DebounceManager;
    metrics: RippleMetrics;
    serverId: string;
    streamConfig?: Partial<StreamConfig>;
    consumerConfig?: Partial<ConsumerConfig>;
    dedupeConfig?: Partial<DedupeConfig>;
  }) {
    this.redis = opts.redis;
    this.logger = opts.createLogger('consumer');
    this.registry = opts.registry;
    this.executor = opts.executor;
    this.dedupe = opts.dedupe;
    this.debounce = opts.debounce;
    this.metrics = opts.metrics;
    this.serverId = opts.serverId;
    this.streamConfig = { ...DEFAULT_STREAM_CONFIG, ...opts.streamConfig };
    this.consumerConfig = {
      ...DEFAULT_CONSUMER_CONFIG,
      ...opts.consumerConfig,
    };
    this.dedupeConfig = { ...DEFAULT_DEDUPE_CONFIG, ...opts.dedupeConfig };
    this.groupName = `group:${this.serverId}`;
  }

  /**
   * Start the consumer loop.
   */
  async start(): Promise<void> {
    this.logger.info('Consumer starting', {
      serverId: this.serverId,
      groupName: this.groupName,
      streamKey: this.streamConfig.key,
    });

    // Create consumer group (idempotent)
    await this.ensureConsumerGroup();

    // Reclaim pending messages from previous crash
    await this.reclaimPending();

    // Start main loop
    this.running = true;
    this.startReclaimTimer();

    // Run loop in background (don't await)
    this.consumeLoop().catch((err) => {
      this.logger.error('Consumer loop crashed', {
        error: err?.message ?? String(err),
      });
    });

    this.logger.info('Consumer started');
  }

  /**
   * Graceful shutdown.
   */
  async stop(): Promise<void> {
    this.logger.info('Consumer stopping', {
      inflightCount: this.inflightCount,
    });

    this.running = false;

    if (this.reclaimTimer) {
      clearInterval(this.reclaimTimer);
      this.reclaimTimer = null;
    }

    // Flush pending debounces
    this.debounce.flushAll();

    // Wait for inflight tasks to complete (with timeout)
    const drainTimeout = 10000;
    const drainStart = Date.now();
    while (this.inflightCount > 0 && Date.now() - drainStart < drainTimeout) {
      await new Promise((r) => setTimeout(r, 100));
    }

    if (this.inflightCount > 0) {
      this.logger.warn('Consumer stopped with inflight tasks', {
        inflightCount: this.inflightCount,
      });
    } else {
      this.logger.info('Consumer stopped cleanly');
    }
  }

  /**
   * Main consume loop.
   */
  private async consumeLoop(): Promise<void> {
    while (this.running) {
      try {
        const response = await (this.redis as any).xreadgroup(
          'GROUP',
          this.groupName,
          this.consumerName,
          'COUNT',
          this.streamConfig.batchSize,
          'BLOCK',
          this.streamConfig.blockMs,
          'STREAMS',
          this.streamConfig.key,
          '>',
        );

        if (!response) continue; // timeout, no new messages

        for (const [_streamKey, entries] of response) {
          for (const [entryId, fields] of entries) {
            await this.processEntry(entryId, fields);
          }
        }
      } catch (err: any) {
        if (!this.running) break; // expected during shutdown

        this.logger.error('Consumer loop error', {
          error: err?.message ?? String(err),
        });

        // Back off before retrying
        await new Promise((r) => setTimeout(r, 1000));
      }
    }
  }

  /**
   * Process a single stream entry.
   */
  private async processEntry(
    entryId: string,
    fields: string[],
  ): Promise<void> {
    const event = this.parseEvent(fields);
    if (!event) {
      this.logger.warn('Invalid stream entry, ACKing to skip', { entryId });
      await this.ack(entryId);
      return;
    }

    const logCtx = {
      requestId: event.requestId,
      pattern: event.pattern,
      entryId,
    };

    // Wildcard match
    const matched = this.registry.match(event.pattern);
    if (matched.length === 0) {
      this.logger.debug('No refreshables matched pattern', logCtx);
      await this.ack(entryId);
      return;
    }

    this.logger.info('Processing event', {
      ...logCtx,
      matchedCount: matched.length,
      matchedKeys: matched.map((r) => r.key),
    });

    this.inflightCount++;

    try {
      for (const refreshable of matched) {
        // Dedupe check
        const isNew = await this.dedupe.markIfNew(
          this.serverId,
          event.requestId,
          refreshable.key,
          this.dedupeConfig.ttlSec,
        );

        if (!isNew) {
          this.metrics.dedupeSkipTotal.inc();
          this.logger.debug('Skipped (dedupe)', { ...logCtx, refreshKey: refreshable.key });
          continue;
        }

        // Debounce (if configured for this refreshable)
        if (refreshable.debounceMs && refreshable.debounceMs > 0) {
          // Schedule debounced execution (non-blocking)
          this.debounce
            .schedule(refreshable.key, event, refreshable.debounceMs)
            .then((finalEvent) => {
              const ctx: RefreshContext = {
                trigger: 'refresh',
                requestId: finalEvent.requestId,
                pattern: finalEvent.pattern,
                startedAt: Date.now(),
              };
              return this.executor.execute(refreshable, ctx);
            })
            .catch((err) => {
              this.logger.error('Debounced execution failed', {
                ...logCtx,
                refreshKey: refreshable.key,
                error: err?.message,
              });
            });
        } else {
          // Execute immediately
          const ctx: RefreshContext = {
            trigger: 'refresh',
            requestId: event.requestId,
            pattern: event.pattern,
            startedAt: Date.now(),
          };

          const result = await this.executor.execute(refreshable, ctx);

          this.logger.info('Refresh result', {
            ...logCtx,
            refreshKey: refreshable.key,
            status: result.status,
            durationMs: result.durationMs,
          });
        }
      }
    } finally {
      this.inflightCount--;
    }

    await this.ack(entryId);
  }

  /**
   * Parse stream entry fields into a RefreshEvent.
   */
  private parseEvent(fields: string[]): RefreshEvent | null {
    const map = new Map<string, string>();
    for (let i = 0; i < fields.length; i += 2) {
      map.set(fields[i], fields[i + 1]);
    }

    const requestId = map.get('requestId');
    const pattern = map.get('pattern');
    const createdAt = map.get('createdAt');

    if (!requestId || !pattern || !createdAt) return null;

    return {
      requestId,
      pattern,
      triggeredBy: map.get('triggeredBy') || undefined,
      createdAt: Number(createdAt),
    };
  }

  /**
   * ACK a stream entry.
   */
  private async ack(entryId: string): Promise<void> {
    await (this.redis as any).xack(
      this.streamConfig.key,
      this.groupName,
      entryId,
    );
  }

  /**
   * Ensure consumer group exists (idempotent).
   */
  private async ensureConsumerGroup(): Promise<void> {
    try {
      await (this.redis as any).xgroup(
        'CREATE',
        this.streamConfig.key,
        this.groupName,
        '$',
        'MKSTREAM',
      );
      this.logger.info('Consumer group created', {
        groupName: this.groupName,
      });
    } catch (err: any) {
      if (err?.message?.includes('BUSYGROUP')) {
        this.logger.debug('Consumer group already exists', {
          groupName: this.groupName,
        });
      } else {
        throw err;
      }
    }
  }

  /**
   * Reclaim pending (unacknowledged) messages from crashed consumers.
   * Uses XPENDING + XCLAIM (ioredis v4 compatible).
   */
  private async reclaimPending(): Promise<void> {
    try {
      const pending: Array<[string, string, number, number]> =
        await (this.redis as any).xpending(
          this.streamConfig.key,
          this.groupName,
          '-',
          '+',
          this.consumerConfig.claimBatchSize,
        );

      if (!pending || pending.length === 0) return;

      // Filter by idle time
      const staleIds = pending
        .filter(([_id, _consumer, idleMs]) => idleMs >= this.consumerConfig.claimMinIdleMs)
        .map(([id]) => id);

      if (staleIds.length === 0) return;

      const claimed: Array<[string, string[]]> = await (this.redis as any).xclaim(
        this.streamConfig.key,
        this.groupName,
        this.consumerName,
        this.consumerConfig.claimMinIdleMs,
        ...staleIds,
      );

      this.metrics.pendingReclaimTotal.inc(claimed.length);

      this.logger.info('Reclaimed pending messages', {
        count: claimed.length,
        ids: staleIds,
      });

      // Re-process claimed messages
      for (const [entryId, fields] of claimed) {
        await this.processEntry(entryId, fields);
      }
    } catch (err: any) {
      this.logger.warn('Pending reclaim failed', {
        error: err?.message ?? String(err),
      });
    }
  }

  /**
   * Start periodic pending reclaim timer.
   */
  private startReclaimTimer(): void {
    this.reclaimTimer = setInterval(() => {
      this.reclaimPending().catch((err) => {
        this.logger.warn('Periodic reclaim failed', {
          error: err?.message ?? String(err),
        });
      });
    }, this.consumerConfig.pendingReclaimIntervalMs);

    // Don't keep process alive just for reclaim
    if (this.reclaimTimer.unref) {
      this.reclaimTimer.unref();
    }
  }
}
