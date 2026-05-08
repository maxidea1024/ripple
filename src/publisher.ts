// ---------------------------------------------------------------------------
// @gatrix/ripple ??Refresh Publisher
// ---------------------------------------------------------------------------

import Redis from 'ioredis';
import { nanoid } from 'nanoid';
import { RippleLogger, RippleLoggerFactory } from './logger';
import { RefreshEvent, StreamConfig, DEFAULT_STREAM_CONFIG } from './types';
import { RippleMetrics } from './metrics';

/**
 * Publishes refresh events to the Redis Stream.
 */
export class RefreshPublisher {
  private readonly redis: Redis.Redis;
  private readonly logger: RippleLogger;
  private readonly metrics: RippleMetrics;
  private readonly streamConfig: StreamConfig;

  constructor(
    redis: Redis.Redis,
    createLogger: RippleLoggerFactory,
    metrics: RippleMetrics,
    streamConfig?: Partial<StreamConfig>,
  ) {
    this.redis = redis;
    this.logger = createLogger('publisher');
    this.metrics = metrics;
    this.streamConfig = { ...DEFAULT_STREAM_CONFIG, ...streamConfig };
  }

  /**
   * Publish a refresh event to the stream.
   *
   * @returns the stream entry ID assigned by Redis.
   */
  async publish(event: RefreshEvent): Promise<string> {
    const fields: (string | number)[] = [
      'requestId',
      event.requestId,
      'pattern',
      event.pattern,
      'triggeredBy',
      event.triggeredBy ?? '',
      'cascade',
      event.cascade ? '1' : '0',
      'createdAt',
      String(event.createdAt),
    ];

    // Serialize metadata as JSON if present
    if (event.metadata && Object.keys(event.metadata).length > 0) {
      fields.push('metadata', JSON.stringify(event.metadata));
    }

    const entryId = await (this.redis as any).xadd(
      this.streamConfig.key,
      'MAXLEN',
      '~',
      String(this.streamConfig.maxLen),
      '*',
      ...fields,
    );

    this.metrics.publishTotal.inc();

    this.logger.info('Event published', {
      requestId: event.requestId,
      pattern: event.pattern,
      triggeredBy: event.triggeredBy,
      entryId,
    });

    return entryId;
  }

  /**
   * Create a RefreshEvent with auto-generated requestId and timestamp.
   */
  static createEvent(
    pattern: string,
    triggeredBy?: string,
    cascade?: boolean,
    metadata?: Record<string, string>,
  ): RefreshEvent {
    return {
      requestId: nanoid(),
      pattern,
      triggeredBy,
      cascade,
      createdAt: Date.now(),
      metadata,
    };
  }
}
