// ---------------------------------------------------------------------------
// @gatrix/ripple ??Lock Tests
// ---------------------------------------------------------------------------

import { DistributedLock } from '../src/lock';
import { RedisMock } from './helpers/redis-mock';
import { createSilentLoggerFactory } from '../src/logger';

describe('DistributedLock', () => {
  let redis: RedisMock;
  let lock: DistributedLock;
  const silentLoggerFactory = createSilentLoggerFactory();
  const serverId = 'server-test';

  beforeEach(() => {
    redis = new RedisMock();
    lock = new DistributedLock(redis as any, silentLoggerFactory);
  });

  describe('acquire', () => {
    it('should acquire a lock successfully', async () => {
      const result = await lock.acquire(serverId, 'event/summer', 5000, 'lock-1');
      expect(result).toBe(true);
    });

    it('should fail to acquire if lock is already held', async () => {
      await lock.acquire(serverId, 'event/summer', 5000, 'lock-1');
      const result = await lock.acquire(serverId, 'event/summer', 5000, 'lock-2');
      expect(result).toBe(false);
    });

    it('should allow acquiring different keys independently', async () => {
      const r1 = await lock.acquire(serverId, 'event/summer', 5000, 'lock-1');
      const r2 = await lock.acquire(serverId, 'event/halloween', 5000, 'lock-2');
      expect(r1).toBe(true);
      expect(r2).toBe(true);
    });

    it('should allow different servers to lock the same key', async () => {
      const r1 = await lock.acquire('server-a', 'event/summer', 5000, 'lock-1');
      const r2 = await lock.acquire('server-b', 'event/summer', 5000, 'lock-2');
      expect(r1).toBe(true);
      expect(r2).toBe(true);
    });
  });

  describe('release', () => {
    it('should release a held lock', async () => {
      await lock.acquire(serverId, 'event/summer', 5000, 'lock-1');
      const released = await lock.release(serverId, 'event/summer', 'lock-1');
      expect(released).toBe(true);
    });

    it('should fail to release with wrong lockId (CAS protection)', async () => {
      await lock.acquire(serverId, 'event/summer', 5000, 'lock-1');
      const released = await lock.release(serverId, 'event/summer', 'wrong-id');
      expect(released).toBe(false);
    });

    it('should allow re-acquire after release', async () => {
      await lock.acquire(serverId, 'event/summer', 5000, 'lock-1');
      await lock.release(serverId, 'event/summer', 'lock-1');
      const result = await lock.acquire(serverId, 'event/summer', 5000, 'lock-2');
      expect(result).toBe(true);
    });

    it('should return false for releasing a non-existent lock', async () => {
      const released = await lock.release(serverId, 'nonexistent', 'lock-1');
      expect(released).toBe(false);
    });
  });

  describe('extend', () => {
    it('should extend a held lock', async () => {
      await lock.acquire(serverId, 'event/summer', 5000, 'lock-1');
      const extended = await lock.extend(serverId, 'event/summer', 'lock-1', 10000);
      expect(extended).toBe(true);
    });

    it('should fail to extend with wrong lockId', async () => {
      await lock.acquire(serverId, 'event/summer', 5000, 'lock-1');
      const extended = await lock.extend(serverId, 'event/summer', 'wrong-id', 10000);
      expect(extended).toBe(false);
    });

    it('should fail to extend a non-existent lock', async () => {
      const extended = await lock.extend(serverId, 'nonexistent', 'lock-1', 10000);
      expect(extended).toBe(false);
    });
  });
});
