// ---------------------------------------------------------------------------
// @gatrix/ripple — Refresh Publisher
//
// Publishes refresh events via Redis Pub/Sub.
// ---------------------------------------------------------------------------

import Redis from 'ioredis';
import { nanoid } from 'nanoid';
import { RippleLogger, RippleLoggerFactory } from './logger';
import { RefreshEvent } from './types';
import { RippleMetrics } from './metrics';

/**
 * Publishes refresh events via Redis Pub/Sub PUBLISH command.
 */
export class RefreshPublisher {
  private readonly redis: Redis.Redis;
  private readonly logger: RippleLogger;
  private readonly metrics: RippleMetrics;
  private readonly channel: string;

  constructor(
    redis: Redis.Redis,
    createLogger: RippleLoggerFactory,
    metrics: RippleMetrics,
    /** Pub/Sub channel name (default: 'ripple:fanout') */
    channel?: string,
  ) {
    this.redis = redis;
    this.logger = createLogger('publisher');
    this.metrics = metrics;
    this.channel = channel ?? 'ripple:fanout';
  }

  /**
   * Publish a refresh event via Pub/Sub.
   *
   * @returns the number of subscribers that received the message.
   */
  async publish(event: RefreshEvent): Promise<number> {
    const message = JSON.stringify(event);

    const receiverCount = await this.redis.publish(this.channel, message);

    this.metrics.publishTotal.inc();

    this.logger.info('Event published', {
      requestId: event.requestId,
      pattern: event.pattern,
      triggeredBy: event.triggeredBy,
      receiverCount,
      channel: this.channel,
    });

    return receiverCount;
  }

  /**
   * Create a RefreshEvent with auto-generated requestId and timestamp.
   */
  static createEvent(
    pattern: string,
    environmentId: string,
    triggeredBy?: string,
    cascade?: boolean,
    metadata?: Record<string, string>,
  ): RefreshEvent {
    return {
      requestId: nanoid(),
      pattern,
      environmentId,
      triggeredBy,
      cascade,
      createdAt: Date.now(),
      metadata,
    };
  }
}
