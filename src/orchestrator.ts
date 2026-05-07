// ---------------------------------------------------------------------------
// @gatrix/ripple ??Orchestrator Facade
// ---------------------------------------------------------------------------

import Redis from 'ioredis';
import { Router } from 'express';
import { RippleLogger, ConsoleLogger, LogLevel } from './logger';
import {
  Refreshable,
  OrchestratorConfig,
  BootstrapResult,
  DEFAULT_BOOTSTRAP_OPTIONS,
} from './types';
import { RefreshableRegistry } from './registry';
import { DistributedLock } from './lock';
import { DedupeChecker } from './dedupe';
import { RefreshExecutor } from './executor';
import { BootstrapLoader } from './bootstrap';
import { RefreshPublisher } from './publisher';
import { StreamConsumer } from './consumer';
import { DebounceManager } from './debounce';
import { RippleMetrics } from './metrics';
import { createRefreshRouter } from './api';

export interface RippleInstance {
  /** Register a refreshable handler. Chainable. */
  register(refreshable: Refreshable): RippleInstance;

  /** Start the orchestrator: validate ??bootstrap ??consumer ??API. */
  start(): Promise<BootstrapResult>;

  /** Graceful shutdown. */
  shutdown(): Promise<void>;

  /** Get the Express router for mounting on an existing app. */
  createRouter(): Router;

  /** Direct access to the publisher for programmatic event publishing. */
  publisher: RefreshPublisher;

  /** Direct access to the registry. */
  registry: RefreshableRegistry;

  /** Direct access to metrics. */
  metrics: RippleMetrics;
}

/**
 * Create a Ripple orchestrator instance.
 *
 * Usage:
 * ```ts
 * const ripple = createRipple({
 *   serverId: 'lobbyd-1',
 *   redis: { host: 'localhost', port: 6379 },
 * });
 *
 * ripple
 *   .register({ key: 'item-table', refresh: async (ctx) => { ... } })
 *   .register({ key: 'shop-config', refresh: async (ctx) => { ... } });
 *
 * await ripple.start();
 * ```
 */
export function createRipple(
  config: OrchestratorConfig,
  externalLogger?: RippleLogger,
): RippleInstance {
  // Logger
  const logger =
    externalLogger ??
    new ConsoleLogger((config.logLevel as LogLevel) ?? 'info');
  const log = logger.child({ module: 'ripple' });

  // Redis connections (two separate: one for commands, one for blocking reads)
  const redisOpts: Redis.RedisOptions = {
    host: config.redis.host,
    port: config.redis.port,
    password: config.redis.password,
    db: config.redis.db,
    keyPrefix: config.redis.keyPrefix,
    lazyConnect: true,
    ...config.redis.options,
  };

  const commandRedis = new Redis(redisOpts);
  const subscriberRedis = new Redis(redisOpts);

  // Core components
  const metrics = new RippleMetrics();
  const registry = new RefreshableRegistry();
  const lock = new DistributedLock(commandRedis, logger);
  const dedupeChecker = new DedupeChecker(commandRedis, logger);
  const debounceManager = new DebounceManager(logger, metrics);

  const executor = new RefreshExecutor({
    lock,
    metrics,
    logger,
    serverId: config.serverId,
    retryConfig: config.retry,
    defaultTimeoutMs: config.defaultTimeoutMs,
  });

  const bootstrapLoader = new BootstrapLoader(registry, executor, logger);

  const publisher = new RefreshPublisher(
    commandRedis,
    logger,
    metrics,
    config.stream,
  );

  const consumer = new StreamConsumer({
    redis: subscriberRedis,
    logger,
    registry,
    executor,
    dedupe: dedupeChecker,
    debounce: debounceManager,
    metrics,
    serverId: config.serverId,
    streamConfig: config.stream,
    consumerConfig: config.consumer,
    dedupeConfig: config.dedupe,
  });

  let started = false;

  const instance: RippleInstance = {
    registry,
    publisher,
    metrics,

    register(refreshable: Refreshable): RippleInstance {
      registry.register(refreshable);
      return instance;
    },

    async start(): Promise<BootstrapResult> {
      if (started) {
        throw new Error('[ripple] Already started');
      }

      log.info('Starting ripple', {
        serverId: config.serverId,
        registeredCount: registry.size,
      });

      // Connect Redis
      await commandRedis.connect();
      await subscriberRedis.connect();
      log.info('Redis connected');

      // Validate dependency graph
      registry.validateDependencies();
      log.info('Dependency graph validated');

      // Print dependency graph in debug mode
      registry.printDependencyGraph(log);

      // Bootstrap
      const bootstrapOpts = {
        ...DEFAULT_BOOTSTRAP_OPTIONS,
        ...config.bootstrap,
      };
      const result = await bootstrapLoader.run(bootstrapOpts);

      if (result.failureCount > 0 && bootstrapOpts.failFast) {
        throw new Error(
          `[ripple] Bootstrap failed: ${result.failureCount} handler(s) failed`,
        );
      }

      // Start consumer
      await consumer.start();

      started = true;
      log.info('Ripple started successfully', {
        serverId: config.serverId,
        registeredCount: registry.size,
        bootstrapDurationMs: result.durationMs,
      });

      return result;
    },

    async shutdown(): Promise<void> {
      log.info('Shutting down ripple');

      // Stop consumer
      await consumer.stop();

      // Disconnect Redis
      commandRedis.disconnect();
      subscriberRedis.disconnect();

      started = false;
      log.info('Ripple shut down');
    },

    createRouter(): Router {
      return createRefreshRouter({
        registry,
        publisher,
        metrics,
        logger,
      });
    },
  };

  return instance;
}
