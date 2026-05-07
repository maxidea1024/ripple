// ---------------------------------------------------------------------------
// @gatrix/ripple ??Consumer Tests
// ---------------------------------------------------------------------------

import { StreamConsumer } from '../src/consumer';
import { RefreshExecutor } from '../src/executor';
import { RefreshableRegistry } from '../src/registry';
import { DistributedLock } from '../src/lock';
import { DedupeChecker } from '../src/dedupe';
import { DebounceManager } from '../src/debounce';
import { RippleMetrics } from '../src/metrics';
import { createSilentLoggerFactory } from '../src/logger';
import { RedisMock } from './helpers/redis-mock';
import { Refreshable, RefreshContext } from '../src/types';

function makeRefreshable(
  key: string,
  handler?: (ctx: RefreshContext) => Promise<void>,
  opts?: Partial<Refreshable>,
): Refreshable {
  return {
    key,
    refresh: handler ?? jest.fn().mockResolvedValue(undefined),
    ...opts,
  };
}

describe('StreamConsumer', () => {
  let redis: RedisMock;
  let registry: RefreshableRegistry;
  let consumer: StreamConsumer;
  let executor: RefreshExecutor;
  let dedupe: DedupeChecker;
  let debounce: DebounceManager;
  let metrics: RippleMetrics;
  const silentLoggerFactory = createSilentLoggerFactory();
  const serverId = 'server-test';

  beforeEach(() => {
    redis = new RedisMock();
    registry = new RefreshableRegistry();
    metrics = new RippleMetrics();
    const lock = new DistributedLock(redis as any, silentLoggerFactory);
    dedupe = new DedupeChecker(redis as any, silentLoggerFactory);
    debounce = new DebounceManager(silentLoggerFactory, metrics);
    executor = new RefreshExecutor({
      lock,
      metrics,
      createLogger: silentLoggerFactory,
      serverId,
      retryConfig: {
        maxRetries: 0,
        retryDelayMs: 0,
        exponentialBackoff: false,
        maxDelayMs: 0,
      },
      defaultTimeoutMs: 5000,
    });

    consumer = new StreamConsumer({
      redis: redis as any,
      createLogger: silentLoggerFactory,
      registry,
      executor,
      dedupe,
      debounce,
      metrics,
      serverId,
      streamConfig: { key: 'refresh-stream', blockMs: 100, batchSize: 10, maxLen: 1000 },
    });
  });

  describe('ensureConsumerGroup', () => {
    it('should create consumer group successfully', async () => {
      // Start will create the group
      await consumer.start();
      // No error = success
      await consumer.stop();
    });
  });

  describe('event processing', () => {
    it('should process published events', async () => {
      const handler = jest.fn().mockResolvedValue(undefined);
      registry.register(makeRefreshable('event/summer', handler));

      await consumer.start();

      // Publish an event
      await redis.xadd(
        'refresh-stream',
        '*',
        'requestId', 'req-1',
        'pattern', 'event/summer',
        'triggeredBy', 'test',
        'createdAt', String(Date.now()),
      );

      // Give consumer time to process
      await new Promise((r) => setTimeout(r, 300));

      await consumer.stop();

      expect(handler).toHaveBeenCalled();
    });
  });

  describe('graceful stop', () => {
    it('should stop cleanly', async () => {
      await consumer.start();
      await consumer.stop();
      // No hanging promises or timers = success
    });
  });
});
