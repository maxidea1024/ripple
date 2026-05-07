// ---------------------------------------------------------------------------
// @gatrix/ripple ??Publisher Tests
// ---------------------------------------------------------------------------

import { RefreshPublisher } from '../src/publisher';
import { RedisMock } from './helpers/redis-mock';
import { SilentLogger } from '../src/logger';
import { RippleMetrics } from '../src/metrics';

describe('RefreshPublisher', () => {
  let redis: RedisMock;
  let publisher: RefreshPublisher;
  let metrics: RippleMetrics;
  const logger = new SilentLogger();

  beforeEach(() => {
    redis = new RedisMock();
    metrics = new RippleMetrics();
    publisher = new RefreshPublisher(redis as any, logger, metrics);
  });

  describe('publish', () => {
    it('should publish event to stream and return entry ID', async () => {
      const event = RefreshPublisher.createEvent('event/*', 'admin');
      const entryId = await publisher.publish(event);

      expect(entryId).toBeDefined();
      expect(typeof entryId).toBe('string');
    });

    it('should increment publish counter', async () => {
      const event = RefreshPublisher.createEvent('event/*');
      await publisher.publish(event);

      const metricsText = await metrics.getMetrics();
      expect(metricsText).toContain('ripple_refresh_publish_total');
    });

    it('should store event fields in stream', async () => {
      const event = RefreshPublisher.createEvent('localization/*', 'cms-webhook');
      await publisher.publish(event);

      const len = await redis.xlen('refresh-stream');
      expect(len).toBe(1);
    });
  });

  describe('createEvent', () => {
    it('should generate unique requestId', () => {
      const e1 = RefreshPublisher.createEvent('a');
      const e2 = RefreshPublisher.createEvent('a');
      expect(e1.requestId).not.toBe(e2.requestId);
    });

    it('should set pattern and triggeredBy', () => {
      const event = RefreshPublisher.createEvent('event/*', 'admin-api');
      expect(event.pattern).toBe('event/*');
      expect(event.triggeredBy).toBe('admin-api');
      expect(event.createdAt).toBeLessThanOrEqual(Date.now());
    });
  });
});
