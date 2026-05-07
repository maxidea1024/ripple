// ---------------------------------------------------------------------------
// @gatrix/ripple ??Bootstrap Tests
// ---------------------------------------------------------------------------

import { BootstrapLoader } from '../src/bootstrap';
import { RefreshExecutor } from '../src/executor';
import { RefreshableRegistry } from '../src/registry';
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

describe('BootstrapLoader', () => {
  let redis: RedisMock;
  let registry: RefreshableRegistry;
  let executor: RefreshExecutor;
  let bootstrap: BootstrapLoader;
  const logger = new SilentLogger();

  beforeEach(() => {
    redis = new RedisMock();
    registry = new RefreshableRegistry();
    const lock = new DistributedLock(redis as any, logger);
    const metrics = new RippleMetrics();
    executor = new RefreshExecutor({
      lock,
      metrics,
      logger,
      serverId: 'server-test',
      retryConfig: { maxRetries: 0, retryDelayMs: 0, exponentialBackoff: false, maxDelayMs: 0 },
      defaultTimeoutMs: 5000,
    });
    bootstrap = new BootstrapLoader(registry, executor, logger);
  });

  it('should execute all refreshables with trigger "bootstrap"', async () => {
    const handler = jest.fn().mockResolvedValue(undefined);
    registry.register(makeRefreshable('a', handler));
    registry.register(makeRefreshable('b', handler));

    const result = await bootstrap.run({ parallel: false, failFast: false });

    expect(result.totalCount).toBe(2);
    expect(result.successCount).toBe(2);
    expect(handler).toHaveBeenCalledTimes(2);

    // All calls should have trigger 'bootstrap'
    for (const call of handler.mock.calls) {
      expect(call[0].trigger).toBe('bootstrap');
    }
  });

  it('should respect dependency order', async () => {
    const order: string[] = [];

    registry.register(
      makeRefreshable('base', async () => { order.push('base'); }),
    );
    registry.register(
      makeRefreshable('dependent', async () => { order.push('dependent'); }, {
        dependsOn: ['base'],
      }),
    );

    await bootstrap.run({ parallel: true, failFast: false });

    expect(order).toEqual(['base', 'dependent']);
  });

  it('should abort on first failure when failFast is true', async () => {
    const order: string[] = [];

    registry.register(
      makeRefreshable('a', async () => { order.push('a'); throw new Error('fail'); }),
    );
    registry.register(
      makeRefreshable('b', async () => { order.push('b'); }),
    );

    const result = await bootstrap.run({
      parallel: false,
      failFast: true,
    });

    expect(result.failureCount).toBeGreaterThan(0);
    // 'b' should not execute since 'a' failed and failFast is true
    expect(order).toEqual(['a']);
  });

  it('should continue on failure when failFast is false', async () => {
    const order: string[] = [];

    registry.register(
      makeRefreshable('a', async () => { order.push('a'); throw new Error('fail'); }),
    );
    registry.register(
      makeRefreshable('b', async () => { order.push('b'); }),
    );

    const result = await bootstrap.run({
      parallel: false,
      failFast: false,
    });

    expect(order).toEqual(['a', 'b']);
    expect(result.failureCount).toBe(1);
    expect(result.successCount).toBe(1);
  });

  it('should run independent refreshables in parallel', async () => {
    let concurrentMax = 0;
    let concurrent = 0;

    const slowHandler = async () => {
      concurrent++;
      concurrentMax = Math.max(concurrentMax, concurrent);
      await new Promise((r) => setTimeout(r, 50));
      concurrent--;
    };

    registry.register(makeRefreshable('a', slowHandler));
    registry.register(makeRefreshable('b', slowHandler));
    registry.register(makeRefreshable('c', slowHandler));

    await bootstrap.run({ parallel: true, concurrency: 10 });

    // All three should have run concurrently
    expect(concurrentMax).toBe(3);
  });
});
