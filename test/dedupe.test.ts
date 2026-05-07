// ---------------------------------------------------------------------------
// @gatrix/ripple ??Dedupe Tests
// ---------------------------------------------------------------------------

import { DedupeChecker } from '../src/dedupe';
import { RedisMock } from './helpers/redis-mock';
import { createSilentLoggerFactory } from '../src/logger';

describe('DedupeChecker', () => {
  let redis: RedisMock;
  let dedupe: DedupeChecker;
  const silentLoggerFactory = createSilentLoggerFactory();
  const serverId = 'server-test';

  beforeEach(() => {
    redis = new RedisMock();
    dedupe = new DedupeChecker(redis as any, silentLoggerFactory);
  });

  describe('markIfNew', () => {
    it('should return true for first occurrence', async () => {
      const isNew = await dedupe.markIfNew(serverId, 'req-1', 'event/summer', 3600);
      expect(isNew).toBe(true);
    });

    it('should return false for duplicate', async () => {
      await dedupe.markIfNew(serverId, 'req-1', 'event/summer', 3600);
      const isNew = await dedupe.markIfNew(serverId, 'req-1', 'event/summer', 3600);
      expect(isNew).toBe(false);
    });

    it('should treat different requestIds as new', async () => {
      await dedupe.markIfNew(serverId, 'req-1', 'event/summer', 3600);
      const isNew = await dedupe.markIfNew(serverId, 'req-2', 'event/summer', 3600);
      expect(isNew).toBe(true);
    });

    it('should treat different refreshKeys as new', async () => {
      await dedupe.markIfNew(serverId, 'req-1', 'event/summer', 3600);
      const isNew = await dedupe.markIfNew(serverId, 'req-1', 'event/halloween', 3600);
      expect(isNew).toBe(true);
    });

    it('should treat different servers as independent', async () => {
      await dedupe.markIfNew('server-a', 'req-1', 'event/summer', 3600);
      const isNew = await dedupe.markIfNew('server-b', 'req-1', 'event/summer', 3600);
      expect(isNew).toBe(true);
    });

    it('should allow re-processing after key expiration', async () => {
      await dedupe.markIfNew(serverId, 'req-1', 'event/summer', 3600);
      redis.expireKey('ripple:done:server-test:req-1:event/summer');
      const isNew = await dedupe.markIfNew(serverId, 'req-1', 'event/summer', 3600);
      expect(isNew).toBe(true);
    });
  });
});
