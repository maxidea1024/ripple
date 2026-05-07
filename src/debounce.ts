// ---------------------------------------------------------------------------
// @gatrix/ripple ??Debounce Manager
// ---------------------------------------------------------------------------

import { RippleLogger } from './logger';
import { RefreshEvent } from './types';
import { RippleMetrics } from './metrics';

interface PendingDebounce {
  timer: ReturnType<typeof setTimeout>;
  latestEvent: RefreshEvent;
  resolve: (event: RefreshEvent) => void;
  mergeCount: number;
}

/**
 * Per-key in-memory debounce manager.
 *
 * When multiple refresh events arrive for the same refreshable key
 * within the debounce window, only the last event triggers execution.
 */
export class DebounceManager {
  private readonly pending = new Map<string, PendingDebounce>();
  private readonly logger: RippleLogger;
  private readonly metrics: RippleMetrics;

  constructor(logger: RippleLogger, metrics: RippleMetrics) {
    this.logger = logger.child({ module: 'debounce' });
    this.metrics = metrics;
  }

  /**
   * Schedule a debounced execution for the given key.
   * If a pending debounce exists for this key, the timer is reset
   * and the event is replaced with the latest one.
   *
   * @returns Promise that resolves with the final event when the debounce window expires.
   */
  schedule(
    refreshKey: string,
    event: RefreshEvent,
    debounceMs: number,
  ): Promise<RefreshEvent> {
    const existing = this.pending.get(refreshKey);

    if (existing) {
      // Merge: reset timer, update event
      clearTimeout(existing.timer);
      existing.latestEvent = event;
      existing.mergeCount++;
      this.metrics.debounceMergeTotal.labels(refreshKey).inc();

      this.logger.debug('Debounce merged', {
        refreshKey,
        mergeCount: existing.mergeCount,
        debounceMs,
      });

      // Reset timer with same resolve
      existing.timer = setTimeout(() => {
        this.pending.delete(refreshKey);
        existing.resolve(existing.latestEvent);
      }, debounceMs);

      // Return a new promise that will resolve when existing resolves
      return new Promise<RefreshEvent>((resolve) => {
        const prevResolve = existing.resolve;
        existing.resolve = (evt) => {
          prevResolve(evt);
          resolve(evt);
        };
      });
    }

    // New debounce
    return new Promise<RefreshEvent>((resolve) => {
      const entry: PendingDebounce = {
        latestEvent: event,
        mergeCount: 0,
        resolve,
        timer: setTimeout(() => {
          this.pending.delete(refreshKey);
          entry.resolve(entry.latestEvent);
        }, debounceMs),
      };
      this.pending.set(refreshKey, entry);
    });
  }

  /**
   * Check if a key has a pending debounce.
   */
  hasPending(refreshKey: string): boolean {
    return this.pending.has(refreshKey);
  }

  /**
   * Flush all pending debounces immediately.
   * Called during graceful shutdown.
   */
  flushAll(): void {
    for (const [key, entry] of this.pending) {
      clearTimeout(entry.timer);
      entry.resolve(entry.latestEvent);
      this.logger.debug('Debounce flushed (shutdown)', { refreshKey: key });
    }
    this.pending.clear();
  }

  /** Number of pending debounces. */
  get pendingCount(): number {
    return this.pending.size;
  }
}
