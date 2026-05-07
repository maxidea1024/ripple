// ---------------------------------------------------------------------------
// @gatrix/ripple ??Prometheus Metrics
// ---------------------------------------------------------------------------

import {
  Registry,
  Histogram,
  Counter,
  Gauge,
  collectDefaultMetrics,
} from 'prom-client';

export class RippleMetrics {
  public readonly registry: Registry;

  /** Refresh execution duration in seconds */
  public readonly duration: Histogram;

  /** Total successful refreshes */
  public readonly successTotal: Counter;

  /** Total failed refreshes */
  public readonly failTotal: Counter;

  /** Total timed-out refreshes */
  public readonly timeoutTotal: Counter;

  /** Currently running refresh handlers */
  public readonly runningCount: Gauge;

  /** Epoch seconds of last successful refresh per key */
  public readonly lastSuccessTimestamp: Gauge;

  /** Total dedupe-skipped executions */
  public readonly dedupeSkipTotal: Counter;

  /** Total debounce-merged events */
  public readonly debounceMergeTotal: Counter;

  /** Total pending messages reclaimed */
  public readonly pendingReclaimTotal: Counter;

  /** Total events published */
  public readonly publishTotal: Counter;

  constructor(prefix = 'ripple_') {
    this.registry = new Registry();
    collectDefaultMetrics({ register: this.registry, prefix });

    this.duration = new Histogram({
      name: `${prefix}refresh_duration_seconds`,
      help: 'Duration of refresh handler execution in seconds',
      labelNames: ['key', 'trigger', 'status'] as const,
      buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30],
      registers: [this.registry],
    });

    this.successTotal = new Counter({
      name: `${prefix}refresh_success_total`,
      help: 'Total number of successful refresh executions',
      labelNames: ['key', 'trigger'] as const,
      registers: [this.registry],
    });

    this.failTotal = new Counter({
      name: `${prefix}refresh_fail_total`,
      help: 'Total number of failed refresh executions',
      labelNames: ['key', 'trigger', 'error_type'] as const,
      registers: [this.registry],
    });

    this.timeoutTotal = new Counter({
      name: `${prefix}refresh_timeout_total`,
      help: 'Total number of timed-out refresh executions',
      labelNames: ['key'] as const,
      registers: [this.registry],
    });

    this.runningCount = new Gauge({
      name: `${prefix}refresh_running_count`,
      help: 'Number of currently running refresh handlers',
      labelNames: ['key'] as const,
      registers: [this.registry],
    });

    this.lastSuccessTimestamp = new Gauge({
      name: `${prefix}refresh_last_success_timestamp`,
      help: 'Unix timestamp of last successful refresh per key',
      labelNames: ['key'] as const,
      registers: [this.registry],
    });

    this.dedupeSkipTotal = new Counter({
      name: `${prefix}refresh_dedupe_skip_total`,
      help: 'Total number of dedupe-skipped executions',
      registers: [this.registry],
    });

    this.debounceMergeTotal = new Counter({
      name: `${prefix}refresh_debounce_merge_total`,
      help: 'Total number of debounce-merged events',
      labelNames: ['key'] as const,
      registers: [this.registry],
    });

    this.pendingReclaimTotal = new Counter({
      name: `${prefix}refresh_pending_reclaim_total`,
      help: 'Total number of pending messages reclaimed',
      registers: [this.registry],
    });

    this.publishTotal = new Counter({
      name: `${prefix}refresh_publish_total`,
      help: 'Total number of events published to stream',
      registers: [this.registry],
    });
  }

  /** Record a completed refresh execution */
  recordExecution(
    key: string,
    trigger: string,
    status: string,
    durationSec: number,
  ): void {
    this.duration.labels(key, trigger, status).observe(durationSec);

    if (status === 'success') {
      this.successTotal.labels(key, trigger).inc();
      this.lastSuccessTimestamp.labels(key).set(Date.now() / 1000);
    } else if (status === 'timeout') {
      this.timeoutTotal.labels(key).inc();
      this.failTotal.labels(key, trigger, 'timeout').inc();
    } else if (status === 'failure') {
      this.failTotal.labels(key, trigger, 'error').inc();
    }
  }

  /** Get all metrics as Prometheus text format */
  async getMetrics(): Promise<string> {
    return this.registry.metrics();
  }

  /** Get content type header for Prometheus */
  getContentType(): string {
    return this.registry.contentType;
  }
}
