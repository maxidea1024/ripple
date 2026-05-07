// ---------------------------------------------------------------------------
// @gatrix/ripple ??Distributed Lock
// ---------------------------------------------------------------------------

import Redis from 'ioredis';
import { RippleLoggerFactory } from './logger';

/**
 * Redis-based distributed lock using SET NX PX.
 *
 * Each lock is identified by a key and protected by a unique `lockId`
 * to prevent one owner from releasing another's lock (CAS release via Lua).
 *
 * Lock keys include serverId so that different servers can independently
 * lock the same refreshable key without interfering with each other.
 */
export class DistributedLock {
  private readonly redis: Redis.Redis;
  private readonly logger: ReturnType<RippleLoggerFactory>;
  private readonly keyPrefix: string;

  /** Lua script: compare-and-swap release */
  private static readonly RELEASE_SCRIPT = `
    if redis.call("get", KEYS[1]) == ARGV[1] then
      return redis.call("del", KEYS[1])
    end
    return 0
  `;

  /** Lua script: compare-and-swap extend */
  private static readonly EXTEND_SCRIPT = `
    if redis.call("get", KEYS[1]) == ARGV[1] then
      return redis.call("pexpire", KEYS[1], ARGV[2])
    end
    return 0
  `;

  constructor(redis: Redis.Redis, createLogger: RippleLoggerFactory, keyPrefix = 'ripple') {
    this.redis = redis;
    this.logger = createLogger('lock');
    this.keyPrefix = keyPrefix;
  }

  private buildKey(serverId: string, refreshKey: string): string {
    return `${this.keyPrefix}:lock:${serverId}:${refreshKey}`;
  }

  /**
   * Attempt to acquire a lock.
   *
   * @returns true on success, false if lock is already held.
   */
  async acquire(
    serverId: string,
    refreshKey: string,
    ttlMs: number,
    lockId: string,
  ): Promise<boolean> {
    const key = this.buildKey(serverId, refreshKey);
    const result = await this.redis.set(key, lockId, 'PX', ttlMs, 'NX');

    if (result === 'OK') {
      this.logger.debug('Lock acquired', { key, lockId, ttlMs });
      return true;
    }

    this.logger.debug('Lock already held', { key, lockId });
    return false;
  }

  /**
   * Release a lock (only if we are the owner).
   * Uses Lua CAS to prevent releasing another owner's lock.
   */
  async release(
    serverId: string,
    refreshKey: string,
    lockId: string,
  ): Promise<boolean> {
    const key = this.buildKey(serverId, refreshKey);
    const result = await (this.redis as any).eval(
      DistributedLock.RELEASE_SCRIPT,
      1,
      key,
      lockId,
    );

    const released = result === 1;
    this.logger.debug('Lock release attempt', { key, lockId, released });
    return released;
  }

  /**
   * Extend a held lock's TTL (only if we are the owner).
   */
  async extend(
    serverId: string,
    refreshKey: string,
    lockId: string,
    ttlMs: number,
  ): Promise<boolean> {
    const key = this.buildKey(serverId, refreshKey);
    const result = await (this.redis as any).eval(
      DistributedLock.EXTEND_SCRIPT,
      1,
      key,
      lockId,
      String(ttlMs),
    );

    const extended = result === 1;
    this.logger.debug('Lock extend attempt', { key, lockId, ttlMs, extended });
    return extended;
  }
}
