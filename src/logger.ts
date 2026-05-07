// ---------------------------------------------------------------------------
// @gatrix/ripple -- Logger Interface & Default Console Logger
// ---------------------------------------------------------------------------

/**
 * Logger interface for ripple.
 * Consumers can inject any logger that satisfies this interface
 * (e.g. pino, winston, bunyan, or a custom implementation).
 */
export interface RippleLogger {
  debug(msg: string, meta?: Record<string, unknown>): void;
  info(msg: string, meta?: Record<string, unknown>): void;
  warn(msg: string, meta?: Record<string, unknown>): void;
  error(msg: string, meta?: Record<string, unknown>): void;
}

/**
 * Factory function that creates a module-scoped logger.
 *
 * Usage by external consumers:
 * ```typescript
 * import mlog from './mlog';
 *
 * const createLogger: RippleLoggerFactory = (module) => ({
 *   debug: (msg, meta) => mlog.debug(`[ripple:${module}] ${msg}`, meta),
 *   info:  (msg, meta) => mlog.info(`[ripple:${module}] ${msg}`, meta),
 *   warn:  (msg, meta) => mlog.warn(`[ripple:${module}] ${msg}`, meta),
 *   error: (msg, meta) => mlog.error(`[ripple:${module}] ${msg}`, meta),
 * });
 * ```
 */
export type RippleLoggerFactory = (module: string) => RippleLogger;

/**
 * Default console-based logger factory.
 * Outputs structured JSON to stdout/stderr.
 */
export function createConsoleLoggerFactory(
  level: LogLevel = 'info',
): RippleLoggerFactory {
  return (module: string): RippleLogger => {
    const bindings = { module };
    return {
      debug(msg: string, meta?: Record<string, unknown>): void {
        if (LOG_LEVELS[level] > LOG_LEVELS.debug) return;
        write('debug', msg, bindings, meta);
      },
      info(msg: string, meta?: Record<string, unknown>): void {
        if (LOG_LEVELS[level] > LOG_LEVELS.info) return;
        write('info', msg, bindings, meta);
      },
      warn(msg: string, meta?: Record<string, unknown>): void {
        if (LOG_LEVELS[level] > LOG_LEVELS.warn) return;
        write('warn', msg, bindings, meta);
      },
      error(msg: string, meta?: Record<string, unknown>): void {
        write('error', msg, bindings, meta);
      },
    };
  };
}

function write(
  level: string,
  msg: string,
  bindings: Record<string, unknown>,
  meta?: Record<string, unknown>,
): void {
  const entry = {
    time: new Date().toISOString(),
    level,
    msg,
    ...bindings,
    ...meta,
  };

  const output = JSON.stringify(entry);

  if (level === 'error') {
    process.stderr.write(output + '\n');
  } else {
    process.stdout.write(output + '\n');
  }
}

export type LogLevel = 'debug' | 'info' | 'warn' | 'error' | 'silent';

const LOG_LEVELS: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
  silent: 4,
};

/** No-op logger factory for tests or when logging is disabled. */
export function createSilentLoggerFactory(): RippleLoggerFactory {
  const silent: RippleLogger = {
    debug() { /* noop */ },
    info() { /* noop */ },
    warn() { /* noop */ },
    error() { /* noop */ },
  };
  return () => silent;
}
