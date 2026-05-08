// ---------------------------------------------------------------------------
// @gatrix/ripple — Express API Router
//
// REST API for triggering refresh events and inspecting registered handlers.
// History is managed by the caller (e.g. Gatrix backend DB), not by Ripple.
// ---------------------------------------------------------------------------

import { Router, Request, Response } from 'express';
import { RippleLoggerFactory } from './logger';
import { RefreshableRegistry } from './registry';
import { RefreshPublisher } from './publisher';
import { RippleMetrics } from './metrics';

/**
 * Creates an Express router for the refresh orchestrator API.
 *
 * Endpoints:
 *   POST /refresh          — Publish a refresh event
 *   GET  /refreshables     — List registered refreshables
 *   GET  /metrics          — Prometheus metrics
 *   GET  /health           — Health check
 */
export function createRefreshRouter(opts: {
  registry: RefreshableRegistry;
  publisher: RefreshPublisher;
  metrics: RippleMetrics;
  createLogger: RippleLoggerFactory;
  environmentId: string;
}): Router {
  const { registry, publisher, metrics, createLogger, environmentId } = opts;
  const log = createLogger('api');
  const router = Router();

  /**
   * POST /refresh
   * Body: { pattern: string, triggeredBy?: string, cascade?: boolean, metadata?: Record<string, string> }
   * Response: { requestId, pattern, matchedKeys, matchedCount, cascade, environmentId, status }
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
      const event = RefreshPublisher.createEvent(pattern, environmentId, triggeredBy, !!cascade, metadata);
      const receiverCount = await publisher.publish(event);

      const matchedKeys = matched.map((r) => r.key);

      log.info('Refresh published via API', {
        requestId: event.requestId,
        pattern,
        triggeredBy,
        environmentId,
        matchedKeys,
        receiverCount,
      });

      return res.json({
        requestId: event.requestId,
        pattern,
        matchedKeys,
        matchedCount: matchedKeys.length,
        cascade: !!cascade,
        environmentId,
        receiverCount,
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
      environmentId,
      items,
    });
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
    return res.json({
      status: 'ok',
      registeredCount: registry.size,
      environmentId,
    });
  });

  return router;
}
