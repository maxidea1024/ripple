// ---------------------------------------------------------------------------
// @gatrix/ripple ??Debounce Tests
// ---------------------------------------------------------------------------

import { DebounceManager } from '../src/debounce';
import { SilentLogger } from '../src/logger';
import { RippleMetrics } from '../src/metrics';
import { RefreshEvent } from '../src/types';

function makeEvent(pattern: string, requestId = 'req-1'): RefreshEvent {
  return {
    requestId,
    pattern,
    createdAt: Date.now(),
  };
}

describe('DebounceManager', () => {
  let debounce: DebounceManager;
  let metrics: RippleMetrics;

  beforeEach(() => {
    metrics = new RippleMetrics();
    debounce = new DebounceManager(new SilentLogger(), metrics);
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('should resolve after debounce window expires', async () => {
    const promise = debounce.schedule('event/summer', makeEvent('event/*'), 300);

    expect(debounce.pendingCount).toBe(1);

    jest.advanceTimersByTime(300);

    const result = await promise;
    expect(result.pattern).toBe('event/*');
    expect(debounce.pendingCount).toBe(0);
  });

  it('should merge multiple events within debounce window', async () => {
    const p1 = debounce.schedule('event/summer', makeEvent('event/*', 'req-1'), 300);

    jest.advanceTimersByTime(100);
    const p2 = debounce.schedule('event/summer', makeEvent('event/*', 'req-2'), 300);

    jest.advanceTimersByTime(100);
    const p3 = debounce.schedule('event/summer', makeEvent('event/*', 'req-3'), 300);

    jest.advanceTimersByTime(300);

    const [r1, r2, r3] = await Promise.all([p1, p2, p3]);
    // All should resolve with the last event
    expect(r1.requestId).toBe('req-3');
    expect(r2.requestId).toBe('req-3');
    expect(r3.requestId).toBe('req-3');
  });

  it('should handle different keys independently', async () => {
    const p1 = debounce.schedule('event/summer', makeEvent('e', 'req-1'), 300);
    const p2 = debounce.schedule('event/halloween', makeEvent('e', 'req-2'), 300);

    expect(debounce.pendingCount).toBe(2);

    jest.advanceTimersByTime(300);

    const [r1, r2] = await Promise.all([p1, p2]);
    expect(r1.requestId).toBe('req-1');
    expect(r2.requestId).toBe('req-2');
  });

  it('should flush all pending on shutdown', async () => {
    const p1 = debounce.schedule('a', makeEvent('a', 'req-1'), 5000);
    const p2 = debounce.schedule('b', makeEvent('b', 'req-2'), 5000);

    debounce.flushAll();

    const [r1, r2] = await Promise.all([p1, p2]);
    expect(r1.requestId).toBe('req-1');
    expect(r2.requestId).toBe('req-2');
    expect(debounce.pendingCount).toBe(0);
  });
});
