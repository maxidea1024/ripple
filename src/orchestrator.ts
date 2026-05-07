// ---------------------------------------------------------------------------
// @gatrix/ripple ??Orchestrator Facade
// ---------------------------------------------------------------------------

import Redis from 'ioredis';
import { hostname } from 'os';
import { Router } from 'express';
import { RippleLogger, RippleLoggerFactory, createConsoleLoggerFactory, LogLevel } from './logger';
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
  loggerFactory?: RippleLoggerFactory,
): RippleInstance {
  // Server identity
  const serverId =
    config.serverId ?? `${hostname()}-${process.pid}`;

  // Logger
  const createLogger: RippleLoggerFactory =
    loggerFactory ??
    createConsoleLoggerFactory((config.logLevel as LogLevel) ?? 'info');
  const log = createLogger('ripple');

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
    config.stream,
  );

  const consumer = new StreamConsumer({
    redis: subscriberRedis,
    createLogger,
    registry,
    executor,
    dedupe: dedupeChecker,
    debounce: debounceManager,
    metrics,
    serverId,
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

      // Verify Redis version (5.0+ required for Streams)
      await verifyRedisVersion(commandRedis, createLogger('version-check'));

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
        createLogger,
      });
    },
  };

  return instance;
}

// ---------------------------------------------------------------------------
// Redis version verification
// ---------------------------------------------------------------------------

const MINIMUM_REDIS_VERSION = [5, 0];

async function verifyRedisVersion(
  redis: InstanceType<typeof Redis>,
  log: RippleLogger,
): Promise<void> {
  // Strategy 1: Try INFO server
  let version: string | null = null;

  try {
    const rawInfo = await redis.info('server');
    const match = rawInfo.match(/redis_version:(\d+)\.(\d+)\.(\d+)/);
    if (match) {
      const major = parseInt(match[1], 10);
      const minor = parseInt(match[2], 10);
      const patch = parseInt(match[3], 10);
      version = `${major}.${minor}.${patch}`;

      const [reqMajor, reqMinor] = MINIMUM_REDIS_VERSION;
      const isCompatible =
        major > reqMajor || (major === reqMajor && minor >= reqMinor);

      if (!isCompatible) {
        logVersionError(log, version);
        throw new Error(
          `[ripple] Incompatible Redis version: ${version} (requires >= ${reqMajor}.${reqMinor}). ` +
          `Redis Streams are not available. See log output for details.`,
        );
      }

      log.info('Redis version verified', {
        version,
        required: `>= ${reqMajor}.${reqMinor}`,
      });
      return;
    }
  } catch (err) {
    // If we already threw our own error, re-throw it
    if (err instanceof Error && err.message.startsWith('[ripple]')) {
      throw err;
    }
    // INFO command may be disabled (managed Redis). Fall through to probe.
    log.info('INFO command unavailable. Probing Streams support directly.');
  }

  // Strategy 2: Probe Streams support with a harmless command
  await probeStreamsSupport(redis, log);
}

/**
 * Probe Redis Streams support by issuing XINFO STREAM on a nonexistent key.
 *
 * - If Streams are supported: Redis returns "ERR no such key" (or similar).
 * - If Streams are NOT supported: Redis returns "ERR unknown command".
 */
async function probeStreamsSupport(
  redis: InstanceType<typeof Redis>,
  log: RippleLogger,
): Promise<void> {
  const probeKey = '__ripple_streams_probe__';
  try {
    await (redis as any).xinfo('STREAM', probeKey);
    // If it succeeds (very unlikely), Streams work
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);

    if (/unknown command/i.test(msg)) {
      logVersionError(log, 'unknown (INFO disabled)');
      throw new Error(
        '[ripple] Redis Streams not supported. The XINFO command returned "unknown command". ' +
        'Redis 5.0+ is required. See log output for details.',
      );
    }

    // "ERR no such key" or similar = Streams commands are recognized
    log.info('Redis Streams support confirmed (via command probe)');
  }
}

function logVersionError(log: RippleLogger, version: string): void {
  const [reqMajor, reqMinor] = MINIMUM_REDIS_VERSION;
  log.error('Redis version check FAILED', {
    detected: version,
    required: `>= ${reqMajor}.${reqMinor}`,
  });
  log.error(
    [
      '--------------------------------------------------------------',
      '  @gatrix/ripple requires Redis 5.0 or later.',
      '',
      `  Detected version : ${version}`,
      `  Required version : >= ${reqMajor}.${reqMinor}`,
      '',
      '  Redis Streams (XADD, XREADGROUP, XGROUP, XACK, XPENDING,',
      '  XCLAIM) were introduced in Redis 5.0 and are essential for',
      '  the event delivery mechanism.',
      '',
      '  How to resolve:',
      '    1. Upgrade your Redis server to 5.0+',
      '    2. Or use Docker: docker run -d -p 6379:6379 redis:7-alpine',
      '--------------------------------------------------------------',
    ].join('\n'),
  );
}
