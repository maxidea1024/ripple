// ---------------------------------------------------------------------------
// @gatrix/ripple ??Bootstrap Loader
// ---------------------------------------------------------------------------

import { RippleLogger } from './logger';
import {
  BootstrapOptions,
  BootstrapResult,
  RefreshContext,
  RefreshResult,
  DEFAULT_BOOTSTRAP_OPTIONS,
} from './types';
import { RefreshableRegistry } from './registry';
import { RefreshExecutor } from './executor';

/**
 * Loads all registered refreshables at server startup.
 *
 * - Respects dependency ordering (topological sort).
 * - Supports parallel execution with concurrency limit.
 * - fail-fast mode aborts on first failure.
 */
export class BootstrapLoader {
  private readonly registry: RefreshableRegistry;
  private readonly executor: RefreshExecutor;
  private readonly logger: RippleLogger;

  constructor(
    registry: RefreshableRegistry,
    executor: RefreshExecutor,
    logger: RippleLogger,
  ) {
    this.registry = registry;
    this.executor = executor;
    this.logger = logger.child({ module: 'bootstrap' });
  }

  /**
   * Run bootstrap loading for all registered refreshables.
   */
  async run(options?: Partial<BootstrapOptions>): Promise<BootstrapResult> {
    const opts = { ...DEFAULT_BOOTSTRAP_OPTIONS, ...options };
    const startTime = Date.now();

    this.logger.info('Bootstrap starting', {
      parallel: opts.parallel,
      concurrency: opts.concurrency,
      failFast: opts.failFast,
      count: this.registry.size,
    });

    const sortedKeys = this.registry.topologicalSort();
    const results: RefreshResult[] = [];
    let aborted = false;

    if (opts.parallel) {
      const layers = this.buildLayers(sortedKeys);

      for (const layer of layers) {
        if (aborted) break;

        const layerResults = await this.executeLayer(
          layer,
          opts.concurrency,
          opts.failFast,
        );
        results.push(...layerResults);

        if (
          opts.failFast &&
          layerResults.some(
            (r) => r.status === 'failure' || r.status === 'timeout',
          )
        ) {
          aborted = true;
        }
      }
    } else {
      for (const key of sortedKeys) {
        if (aborted) break;

        const refreshable = this.registry.get(key)!;
        const ctx: RefreshContext = {
          trigger: 'bootstrap',
          startedAt: Date.now(),
        };

        const result = await this.executor.execute(refreshable, ctx);
        results.push(result);

        if (
          opts.failFast &&
          (result.status === 'failure' || result.status === 'timeout')
        ) {
          aborted = true;
        }
      }
    }

    const durationMs = Date.now() - startTime;
    const successCount = results.filter((r) => r.status === 'success').length;
    const failureCount = results.filter(
      (r) => r.status === 'failure' || r.status === 'timeout',
    ).length;

    const bootstrapResult: BootstrapResult = {
      totalCount: results.length,
      successCount,
      failureCount,
      durationMs,
      results,
    };

    if (aborted) {
      this.logger.error('Bootstrap aborted (failFast)', {
        durationMs,
        successCount,
        failureCount,
      });
    } else {
      this.logger.info('Bootstrap completed', {
        durationMs,
        successCount,
        failureCount,
      });
    }

    return bootstrapResult;
  }

  /**
   * Build dependency layers for parallel execution.
   */
  private buildLayers(sortedKeys: string[]): string[][] {
    const layers: string[][] = [];
    const assigned = new Set<string>();

    while (assigned.size < sortedKeys.length) {
      const layer: string[] = [];
      for (const key of sortedKeys) {
        if (assigned.has(key)) continue;

        const refreshable = this.registry.get(key)!;
        const depsResolved =
          !refreshable.dependsOn ||
          refreshable.dependsOn.every((dep) => assigned.has(dep));

        if (depsResolved) {
          layer.push(key);
        }
      }

      for (const key of layer) {
        assigned.add(key);
      }
      layers.push(layer);
    }

    return layers;
  }

  /**
   * Execute a layer of refreshables with concurrency limit.
   */
  private async executeLayer(
    keys: string[],
    concurrency: number,
    failFast: boolean,
  ): Promise<RefreshResult[]> {
    const results: RefreshResult[] = [];
    let aborted = false;

    for (let i = 0; i < keys.length; i += concurrency) {
      if (aborted) break;

      const chunk = keys.slice(i, i + concurrency);
      const chunkResults = await Promise.all(
        chunk.map(async (key) => {
          if (aborted) {
            return {
              key,
              status: 'skipped' as const,
              durationMs: 0,
            };
          }

          const refreshable = this.registry.get(key)!;
          const ctx: RefreshContext = {
            trigger: 'bootstrap',
            startedAt: Date.now(),
          };

          const result = await this.executor.execute(refreshable, ctx);

          if (
            failFast &&
            (result.status === 'failure' || result.status === 'timeout')
          ) {
            aborted = true;
          }

          return result;
        }),
      );

      results.push(...chunkResults);
    }

    return results;
  }
}
