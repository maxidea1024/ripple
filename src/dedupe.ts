// ---------------------------------------------------------------------------
// @gatrix/ripple ??Deduplication Checker
// ---------------------------------------------------------------------------

import Redis from 'ioredis';
import { RippleLoggerFactory } from './logger';

/**
 * Prevents duplicate execution of the same (requestId, refreshKey)
 * combination on a given server.
 *
 * Uses Redis SET NX EX for atomic check-and-mark.
 */
export class DedupeChecker {
  private readonly redis: Redis.Redis;
  private readonly logger: ReturnType<RippleLoggerFactory>;
  private readonly keyPrefix: string;

  constructor(redis: Redis.Redis, createLogger: RippleLoggerFactory, keyPrefix = 'ripple') {
    this.redis = redis;
    this.logger = createLogger('dedupe');
    this.keyPrefix = keyPrefix;
  }

  private buildKey(
    serverId: string,
    requestId: string,
    refreshKey: string,
  ): string {
    return `${this.keyPrefix}:done:${serverId}:${requestId}:${refreshKey}`;
  }

  /**
   * Atomically check if this combination has been processed,
   * and mark it if not.
   *
   * @returns true if this is a NEW (not yet processed) combination.
   *          false if already processed (duplicate).
   */
  async markIfNew(
    serverId: string,
    requestId: string,
    refreshKey: string,
    ttlSec: number,
  ): Promise<boolean> {
    const key = this.buildKey(serverId, requestId, refreshKey);
    const result = await this.redis.set(key, '1', 'EX', ttlSec, 'NX');

    const isNew = result === 'OK';
    if (!isNew) {
      this.logger.debug('Dedupe: already processed', {
        serverId,
        requestId,
        refreshKey,
      });
    }
    return isNew;
  }
}
