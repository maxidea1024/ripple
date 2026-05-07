// ---------------------------------------------------------------------------
// @gatrix/ripple ??Orchestrator Tests
// ---------------------------------------------------------------------------

import { createRipple } from '../src/orchestrator';
import { createSilentLoggerFactory } from '../src/logger';
import { OrchestratorConfig, RefreshContext } from '../src/types';

// Mock ioredis
jest.mock('ioredis', () => {
  const { RedisMock } = require('./helpers/redis-mock');

  return function (_opts?: any) {
    const mock = new RedisMock();
    mock.connect = jest.fn().mockResolvedValue(undefined);
    mock.disconnect = jest.fn();
    return mock;
  };
});

describe('createRipple', () => {
  const config: OrchestratorConfig = {
    serverId: 'test-server-1',
    redis: { host: 'localhost', port: 6379 },
    logLevel: 'silent',
    bootstrap: { failFast: false },
  };
  const silentLoggerFactory = createSilentLoggerFactory();

  it('should create a ripple instance with chainable register', () => {
    const ripple = createRipple(config, silentLoggerFactory);

    const result = ripple
      .register({
        key: 'item-table',
        refresh: jest.fn().mockResolvedValue(undefined),
      })
      .register({
        key: 'shop-config',
        refresh: jest.fn().mockResolvedValue(undefined),
      });

    expect(result).toBe(ripple);
    expect(ripple.registry.size).toBe(2);
  });

  it('should throw on duplicate registration', () => {
    const ripple = createRipple(config, silentLoggerFactory);

    ripple.register({
      key: 'item-table',
      refresh: jest.fn().mockResolvedValue(undefined),
    });

    expect(() =>
      ripple.register({
        key: 'item-table',
        refresh: jest.fn().mockResolvedValue(undefined),
      }),
    ).toThrow('Duplicate');
  });

  it('should start and bootstrap successfully', async () => {
    const ripple = createRipple(config, silentLoggerFactory);

    const handler = jest.fn().mockResolvedValue(undefined);
    ripple.register({ key: 'item-table', refresh: handler });

    const result = await ripple.start();

    expect(result.totalCount).toBe(1);
    expect(result.successCount).toBe(1);
    expect(handler).toHaveBeenCalledTimes(1);

    // Handler should be called with trigger 'bootstrap'
    const ctx = handler.mock.calls[0][0] as RefreshContext;
    expect(ctx.trigger).toBe('bootstrap');

    await ripple.shutdown();
  });

  it('should throw if started twice', async () => {
    const ripple = createRipple(config, silentLoggerFactory);
    ripple.register({
      key: 'a',
      refresh: jest.fn().mockResolvedValue(undefined),
    });

    await ripple.start();
    await expect(ripple.start()).rejects.toThrow('Already started');
    await ripple.shutdown();
  });

  it('should create an Express router', () => {
    const ripple = createRipple(config, silentLoggerFactory);
    const router = ripple.createRouter();

    expect(router).toBeDefined();
    // Router should have the expected routes
    const routes = (router as any).stack
      ?.map((layer: any) => layer.route?.path)
      .filter(Boolean);

    expect(routes).toContain('/refresh');
    expect(routes).toContain('/refreshables');
    expect(routes).toContain('/metrics');
    expect(routes).toContain('/health');
  });

  it('should handle dependency validation on start', async () => {
    const ripple = createRipple(config, silentLoggerFactory);

    ripple.register({
      key: 'a',
      refresh: jest.fn().mockResolvedValue(undefined),
      dependsOn: ['nonexistent'],
    });

    await expect(ripple.start()).rejects.toThrow('depends on unknown key');
  });
});
