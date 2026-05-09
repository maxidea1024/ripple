// ---------------------------------------------------------------------------
// @gatrix/ripple — Orchestrator Facade
//
// Creates and wires all Ripple components together.
// Uses Redis Pub/Sub for event delivery (no Redis Streams).
// ---------------------------------------------------------------------------

import Redis from 'ioredis';
import { hostname } from 'os';
import { Router } from 'express';
import { RippleLoggerFactory, createConsoleLoggerFactory, LogLevel } from './logger';
import {
  Refreshable,
  OrchestratorConfig,
  BootstrapResult,
  DEFAULT_BOOTSTRAP_OPTIONS,
  DEFAULT_PUBSUB_CONFIG,
} from './types';
import { RefreshableRegistry } from './registry';
import { DistributedLock } from './lock';
import { DedupeChecker } from './dedupe';
import { RefreshExecutor } from './executor';
import { BootstrapLoader } from './bootstrap';
import { RefreshPublisher } from './publisher';
import { PubSubConsumer } from './consumer';
import { DebounceManager } from './debounce';
import { RippleMetrics } from './metrics';
import { createRefreshRouter } from './api';
import { validateConfig, verifyServerIdUniqueness } from './validateConfig';

export interface RippleInstance {
  /** Register a refreshable handler. Chainable. */
  register(refreshable: Refreshable): RippleInstance;

  /** Start the orchestrator: validate → bootstrap → consumer → API. */
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
 * Uses Redis Pub/Sub for real-time event delivery.
 * Each environment is isolated via its own Pub/Sub channel.
 *
 * Usage:
 * ```ts
 * const ripple = createRipple({
 *   serviceType: 'lobbyd',
 *   environmentId: 'prod-kr',
 *   redis: { host: 'localhost', port: 6379 },
 * });
 *
 * ripple
 *   .register({ key: 'cms/init', refresh: async (ctx) => { ... } })
 *   .register({ key: 'cms/reload', refresh: async (ctx) => { ... } });
 *
 * await ripple.start();
 * ```
 */
export function createRipple(
  config: OrchestratorConfig,
  loggerFactory?: RippleLoggerFactory,
): RippleInstance {
  // Server identity
  const serverId =
    config.serverId
    ?? process.env.POD_NAME
    ?? `${hostname()}-${process.pid}`;

  // Logger
  const createLogger: RippleLoggerFactory =
    loggerFactory ??
    createConsoleLoggerFactory((config.logLevel as LogLevel) ?? 'info');
  const log = createLogger('ripple');

  // Pub/Sub channel scoped by environment
  const pubsubConfig = { ...DEFAULT_PUBSUB_CONFIG, ...config.pubsub };
  const channel = `${pubsubConfig.channel}:${config.environmentId}`;

  // Redis connections (two separate: one for commands, one for subscriber)
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
  const lock = new DistributedLock(commandRedis, createLogger);
  const dedupeChecker = new DedupeChecker(commandRedis, createLogger);
  const debounceManager = new DebounceManager(createLogger, metrics);

  const executor = new RefreshExecutor({
    lock,
    metrics,
    createLogger,
    serverId,
    retryConfig: config.retry,
    defaultTimeoutMs: config.defaultTimeoutMs,
  });

  const bootstrapLoader = new BootstrapLoader(registry, executor, createLogger);

  const publisher = new RefreshPublisher(
    commandRedis,
    createLogger,
    metrics,
    channel,
  );

  const consumer = new PubSubConsumer({
    redis: subscriberRedis,
    createLogger,
    registry,
    executor,
    dedupe: dedupeChecker,
    debounce: debounceManager,
    metrics,
    serverId,
    serviceType: config.serviceType,
    environmentId: config.environmentId,
    channel,
    dedupeConfig: config.dedupe,
    onExecutionComplete: config.onExecutionComplete,
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
        serverId,
        environmentId: config.environmentId,
        channel,
        registeredCount: registry.size,
      });

      // Phase 1: Validate config (before any I/O)
      validateConfig(config, registry.size, log);

      // Phase 2: Connect Redis
      await commandRedis.connect();
      await subscriberRedis.connect();
      log.info('Redis connected');

      // Phase 3: Runtime checks (requires Redis)
      await verifyServerIdUniqueness(commandRedis, serverId, log);

      // Phase 4: Validate dependency graph
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

      // Start Pub/Sub consumer
      await consumer.start();

      started = true;
      log.info('Ripple started successfully', {
        serverId,
        environmentId: config.environmentId,
        channel,
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
        createLogger,
        environmentId: config.environmentId,
        onRefreshPublished: config.onRefreshPublished,
      });
    },
  };

  return instance;
}
