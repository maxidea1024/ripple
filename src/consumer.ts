// ---------------------------------------------------------------------------
// @gatrix/ripple — Pub/Sub Consumer
//
// Subscribes to a Redis Pub/Sub channel and processes refresh events.
// Replaces the previous Stream-based consumer (XREADGROUP/Consumer Groups).
//
// Why Pub/Sub over Streams:
//   - Broadcast to N servers = O(1) PUBLISH vs O(N) Consumer Groups
//   - No PEL memory overhead (was ~600MB at 500 servers)
//   - No XACK per server per event
//   - Message loss during downtime is safe: bootstrap loads all data on start
// ---------------------------------------------------------------------------

import Redis from 'ioredis';
import { RippleLogger, RippleLoggerFactory } from './logger';
import {
  RefreshEvent,
  RefreshContext,
  DedupeConfig,
  DEFAULT_DEDUPE_CONFIG,
} from './types';
import { RefreshableRegistry } from './registry';
import { RefreshExecutor } from './executor';
import { DedupeChecker } from './dedupe';
import { DebounceManager } from './debounce';
import { RippleMetrics } from './metrics';

export interface PubSubConsumerOptions {
  /** Redis connection for subscribing (will be put into subscriber mode) */
  redis: Redis.Redis;
  createLogger: RippleLoggerFactory;
  registry: RefreshableRegistry;
  executor: RefreshExecutor;
  dedupe: DedupeChecker;
  debounce: DebounceManager;
  metrics: RippleMetrics;
  serverId: string;
  serviceType: string;
  /** Pub/Sub channel name (default: 'ripple:fanout') */
  channel?: string;
  dedupeConfig?: Partial<DedupeConfig>;
}

/**
 * Consumes refresh events via Redis Pub/Sub.
 *
 * - Subscribes to a single channel for all refresh events.
 * - Processes events: dedupe → wildcard match → debounce → execute.
 * - No Consumer Groups, no PEL, no XACK.
 */
export class PubSubConsumer {
  private readonly redis: Redis.Redis;
  private readonly logger: RippleLogger;
  private readonly registry: RefreshableRegistry;
  private readonly executor: RefreshExecutor;
  private readonly dedupe: DedupeChecker;
  private readonly debounce: DebounceManager;
  private readonly metrics: RippleMetrics;
  private readonly serverId: string;
  private readonly channel: string;
  private readonly dedupeConfig: DedupeConfig;

  private running = false;
  private inflightCount = 0;

  constructor(opts: PubSubConsumerOptions) {
    this.redis = opts.redis;
    this.logger = opts.createLogger('consumer');
    this.registry = opts.registry;
    this.executor = opts.executor;
    this.dedupe = opts.dedupe;
    this.debounce = opts.debounce;
    this.metrics = opts.metrics;
    this.serverId = opts.serverId;
    this.channel = opts.channel ?? 'ripple:fanout';
    this.dedupeConfig = { ...DEFAULT_DEDUPE_CONFIG, ...opts.dedupeConfig };
  }

  /**
   * Start listening for Pub/Sub messages.
   */
  async start(): Promise<void> {
    this.logger.info('Consumer starting (Pub/Sub)', {
      serverId: this.serverId,
      channel: this.channel,
    });

    this.running = true;

    this.redis.on('message', (_channel: string, message: string) => {
      this.handleMessage(message).catch((err) => {
        this.logger.error('Failed to handle Pub/Sub message', {
          error: err?.message ?? String(err),
        });
      });
    });

    await this.redis.subscribe(this.channel);

    this.logger.info('Consumer started, subscribed to channel', {
      channel: this.channel,
    });
  }

  /**
   * Graceful shutdown.
   */
  async stop(): Promise<void> {
    this.logger.info('Consumer stopping', {
      inflightCount: this.inflightCount,
    });

    this.running = false;

    try {
      await this.redis.unsubscribe(this.channel);
    } catch {
      // Ignore unsubscribe errors during shutdown
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
   * Handle a single Pub/Sub message.
   */
  private async handleMessage(message: string): Promise<void> {
    if (!this.running) return;

    const event = this.parseEvent(message);
    if (!event) {
      this.logger.warn('Invalid Pub/Sub message, skipping', {
        messagePreview: message.substring(0, 200),
      });
      return;
    }

    const logCtx = {
      requestId: event.requestId,
      pattern: event.pattern,
    };

    // Wildcard match
    let matched = this.registry.match(event.pattern);
    if (matched.length === 0) {
      this.logger.debug('No refreshables matched pattern', logCtx);
      return;
    }

    // Cascade: expand with transitive dependents (topological order)
    if (event.cascade) {
      const initialKeys = matched.map((r) => r.key);
      matched = this.registry.collectCascade(initialKeys);

      this.logger.info('Cascade expanded', {
        ...logCtx,
        initialKeys,
        expandedKeys: matched.map((r) => r.key),
      });
    }

    this.logger.info('Processing event', {
      ...logCtx,
      cascade: event.cascade ?? false,
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
          this.debounce
            .schedule(refreshable.key, event, refreshable.debounceMs)
            .then((finalEvent) => {
              const ctx: RefreshContext = {
                trigger: 'refresh',
                requestId: finalEvent.requestId,
                pattern: finalEvent.pattern,
                startedAt: Date.now(),
                metadata: finalEvent.metadata,
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
            metadata: event.metadata,
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
  }

  /**
   * Parse a JSON message into a RefreshEvent.
   */
  private parseEvent(message: string): RefreshEvent | null {
    try {
      const parsed = JSON.parse(message);
      if (!parsed.requestId || !parsed.pattern || !parsed.createdAt) {
        return null;
      }
      return parsed as RefreshEvent;
    } catch {
      return null;
    }
  }
}
