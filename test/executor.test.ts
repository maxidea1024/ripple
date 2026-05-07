// ---------------------------------------------------------------------------
// @gatrix/ripple ??Executor Tests
// ---------------------------------------------------------------------------

import { RefreshExecutor } from '../src/executor';
import { DistributedLock } from '../src/lock';
import { RippleMetrics } from '../src/metrics';
import { SilentLogger } from '../src/logger';
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

describe('RefreshExecutor', () => {
  let redis: RedisMock;
  let lock: DistributedLock;
  let metrics: RippleMetrics;
  let executor: RefreshExecutor;
  const logger = new SilentLogger();
  const serverId = 'server-test';

  beforeEach(() => {
    redis = new RedisMock();
    lock = new DistributedLock(redis as any, logger);
    metrics = new RippleMetrics();
    executor = new RefreshExecutor({
      lock,
      metrics,
      logger,
      serverId,
      retryConfig: {
        maxRetries: 2,
        retryDelayMs: 10, // fast for tests
        exponentialBackoff: false,
        maxDelayMs: 100,
      },
      defaultTimeoutMs: 5000,
    });
  });

  describe('successful execution', () => {
    it('should execute and return success', async () => {
      const handler = jest.fn().mockResolvedValue(undefined);
      const refreshable = makeRefreshable('item-table', handler);

      const result = await executor.execute(refreshable, {
        trigger: 'refresh',
        requestId: 'req-1',
        startedAt: Date.now(),
      });

      expect(result.status).toBe('success');
      expect(result.key).toBe('item-table');
      expect(handler).toHaveBeenCalledTimes(1);
    });

    it('should pass correct context to handler', async () => {
      const handler = jest.fn().mockResolvedValue(undefined);
      const refreshable = makeRefreshable('item-table', handler);

      await executor.execute(refreshable, {
        trigger: 'bootstrap',
        startedAt: Date.now(),
      });

      const ctx = handler.mock.calls[0][0] as RefreshContext;
      expect(ctx.trigger).toBe('bootstrap');
    });
  });

  describe('failure + retry', () => {
    it('should retry on failure and eventually succeed', async () => {
      let attempts = 0;
      const handler = jest.fn().mockImplementation(async () => {
        attempts++;
        if (attempts < 2) throw new Error('temporary failure');
      });

      const refreshable = makeRefreshable('item-table', handler);

      const result = await executor.execute(refreshable, {
        trigger: 'refresh',
        requestId: 'req-1',
        startedAt: Date.now(),
      });

      expect(result.status).toBe('success');
      expect(handler).toHaveBeenCalledTimes(2);

      // Second call should have trigger 'retry'
      const retryCtx = handler.mock.calls[1][0] as RefreshContext;
      expect(retryCtx.trigger).toBe('retry');
      expect(retryCtx.retryCount).toBe(1);
    });

    it('should return failure after exhausting retries', async () => {
      const handler = jest
        .fn()
        .mockRejectedValue(new Error('persistent failure'));

      const refreshable = makeRefreshable('item-table', handler);

      const result = await executor.execute(refreshable, {
        trigger: 'refresh',
        requestId: 'req-1',
        startedAt: Date.now(),
      });

      expect(result.status).toBe('failure');
      expect(result.error).toBe('persistent failure');
      // 1 initial + 2 retries = 3
      expect(handler).toHaveBeenCalledTimes(3);
    });

    it('should NOT retry during bootstrap', async () => {
      const handler = jest
        .fn()
        .mockRejectedValue(new Error('bootstrap failure'));

      const refreshable = makeRefreshable('item-table', handler);

      const result = await executor.execute(refreshable, {
        trigger: 'bootstrap',
        startedAt: Date.now(),
      });

      expect(result.status).toBe('failure');
      expect(handler).toHaveBeenCalledTimes(1); // No retry
    });
  });

  describe('timeout', () => {
    it('should return timeout when handler exceeds timeout', async () => {
      const handler = jest.fn().mockImplementation(
        () => new Promise((resolve) => setTimeout(resolve, 10000)),
      );

      const refreshable = makeRefreshable('slow-handler', handler, {
        timeoutMs: 50,
      });

      const result = await executor.execute(refreshable, {
        trigger: 'refresh',
        requestId: 'req-1',
        startedAt: Date.now(),
      });

      expect(result.status).toBe('timeout');
    });
  });

  describe('lock contention', () => {
    it('should skip when lock is already held', async () => {
      // Pre-acquire the lock
      await lock.acquire(serverId, 'item-table', 60000, 'other-lock');

      const handler = jest.fn().mockResolvedValue(undefined);
      const refreshable = makeRefreshable('item-table', handler);

      const result = await executor.execute(refreshable, {
        trigger: 'refresh',
        requestId: 'req-1',
        startedAt: Date.now(),
      });

      expect(result.status).toBe('skipped');
      expect(handler).not.toHaveBeenCalled();
    });
  });
});
