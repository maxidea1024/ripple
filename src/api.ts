// ---------------------------------------------------------------------------
// @gatrix/ripple ??Express API Router
// ---------------------------------------------------------------------------

import { Router, Request, Response } from 'express';
import { RippleLoggerFactory } from './logger';
import { RefreshableRegistry } from './registry';
import { RefreshPublisher } from './publisher';
import { RippleMetrics } from './metrics';

import Redis from 'ioredis';

/**
 * Creates an Express router for the refresh orchestrator API.
 *
 * Endpoints:
 *   POST /refresh          ??Publish a refresh event
 *   GET  /refreshables     ??List registered refreshables
 *   GET  /metrics          ??Prometheus metrics
 *   GET  /health           ??Health check
 *   GET  /history          ??Get event history
 */
export function createRefreshRouter(opts: {
  registry: RefreshableRegistry;
  publisher: RefreshPublisher;
  metrics: RippleMetrics;
  createLogger: RippleLoggerFactory;
  redis?: Redis.Redis;
  historyConfig?: import('./types').HistoryConfig;
}): Router {
  const { registry, publisher, metrics, createLogger, redis, historyConfig } = opts;
  const log = createLogger('api');
  const router = Router();

  /**
   * POST /refresh
   * Body: { pattern: string, triggeredBy?: string, cascade?: boolean, metadata?: Record<string, string> }
   * Response: { requestId, pattern, matchedKeys, matchedCount, cascade, status }
   */
  router.post('/refresh', async (req: Request, res: Response) => {
    try {
      const { pattern, triggeredBy, cascade, metadata } = req.body;

      if (!pattern || typeof pattern !== 'string') {
        return res.status(400).json({
          error: 'Missing or invalid "pattern" field',
        });
      }

      // Match against registered refreshables
      const matched = registry.match(pattern);

      if (matched.length === 0) {
        return res.status(404).json({
          error: 'No refreshables match pattern',
          pattern,
        });
      }

      // Create and publish event
      const event = RefreshPublisher.createEvent(pattern, triggeredBy, !!cascade, metadata);
      await publisher.publish(event);

      const matchedKeys = matched.map((r) => r.key);

      log.info('Refresh published via API', {
        requestId: event.requestId,
        pattern,
        triggeredBy,
        matchedKeys,
      });

      return res.json({
        requestId: event.requestId,
        pattern,
        matchedKeys,
        matchedCount: matchedKeys.length,
        cascade: !!cascade,
        status: 'published',
      });
    } catch (err: any) {
      log.error('Refresh API error', { error: err?.message });
      return res.status(500).json({
        error: 'Internal server error',
      });
    }
  });

  /**
   * GET /refreshables
   * Response: list of registered refreshable keys and their config.
   */
  router.get('/refreshables', (_req: Request, res: Response) => {
    const items = registry.getAll().map((r) => ({
      key: r.key,
      timeoutMs: r.timeoutMs,
      debounceMs: r.debounceMs,
      dependsOn: r.dependsOn,
    }));

    return res.json({
      count: items.length,
      items,
    });
  });

  /**
   * GET /history
   * Query: requestId (optional), limit (optional)
   */
  router.get('/history', async (req: Request, res: Response) => {
    if (!redis || !historyConfig) {
      return res.status(501).json({ error: 'History tracking not configured' });
    }
    
    try {
      const requestId = req.query.requestId as string;
      const limit = parseInt((req.query.limit as string) || '100', 10);
      
      const scanCount = requestId ? historyConfig.maxLen : limit;
      const entries = await (redis as any).xrevrange(historyConfig.key, '+', '-', 'COUNT', scanCount);
      const items: import('./types').RippleHistoryEvent[] = [];
      
      for (const [_id, fields] of entries) {
        const obj: any = {};
        for (let i = 0; i < fields.length; i += 2) {
          obj[fields[i]] = fields[i + 1];
        }
        
        // Convert numbers
        obj.durationMs = Number(obj.durationMs);
        obj.delayMs = Number(obj.delayMs);
        obj.retryCount = Number(obj.retryCount);
        obj.createdAt = Number(obj.createdAt);
        obj.startedAt = Number(obj.startedAt);
        obj.finishedAt = Number(obj.finishedAt);
        
        if (requestId && obj.requestId !== requestId) {
          continue;
        }
        
        items.push(obj as import('./types').RippleHistoryEvent);
        if (items.length >= limit) break;
      }
      
      return res.json({ items });
    } catch (err: any) {
      log.error('History API error', { error: err?.message });
      return res.status(500).json({ error: 'Failed to fetch history' });
    }
  });

  /**
   * GET /metrics
   * Prometheus text format metrics.
   */
  router.get('/metrics', async (_req: Request, res: Response) => {
    try {
      const metricsText = await metrics.getMetrics();
      res.set('Content-Type', metrics.getContentType());
      return res.send(metricsText);
    } catch (err: any) {
      log.error('Metrics endpoint error', { error: err?.message });
      return res.status(500).json({ error: 'Failed to collect metrics' });
    }
  });

  /**
   * GET /health
   */
  router.get('/health', (_req: Request, res: Response) => {
    return res.json({ status: 'ok', registeredCount: registry.size });
  });

  return router;
}
