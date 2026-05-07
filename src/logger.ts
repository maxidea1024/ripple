// ---------------------------------------------------------------------------
// @gatrix/ripple ??Logger Interface & Default Console Logger
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

  /** Create a child logger with additional bound context */
  child(bindings: Record<string, unknown>): RippleLogger;
}

/**
 * Simple console-based logger.
 * Outputs structured JSON to stdout/stderr.
 */
export class ConsoleLogger implements RippleLogger {
  private readonly bindings: Record<string, unknown>;
  private readonly level: LogLevel;

  constructor(
    level: LogLevel = 'info',
    bindings: Record<string, unknown> = {},
  ) {
    this.level = level;
    this.bindings = bindings;
  }

  debug(msg: string, meta?: Record<string, unknown>): void {
    if (LOG_LEVELS[this.level] > LOG_LEVELS.debug) return;
    this.write('debug', msg, meta);
  }

  info(msg: string, meta?: Record<string, unknown>): void {
    if (LOG_LEVELS[this.level] > LOG_LEVELS.info) return;
    this.write('info', msg, meta);
  }

  warn(msg: string, meta?: Record<string, unknown>): void {
    if (LOG_LEVELS[this.level] > LOG_LEVELS.warn) return;
    this.write('warn', msg, meta);
  }

  error(msg: string, meta?: Record<string, unknown>): void {
    this.write('error', msg, meta);
  }

  child(bindings: Record<string, unknown>): RippleLogger {
    return new ConsoleLogger(this.level, {
      ...this.bindings,
      ...bindings,
    });
  }

  private write(
    level: string,
    msg: string,
    meta?: Record<string, unknown>,
  ): void {
    const entry = {
      time: new Date().toISOString(),
      level,
      msg,
      ...this.bindings,
      ...meta,
    };

    const output = JSON.stringify(entry);

    if (level === 'error') {
      process.stderr.write(output + '\n');
    } else {
      process.stdout.write(output + '\n');
    }
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

/** No-op logger for tests or when logging is disabled. */
export class SilentLogger implements RippleLogger {
  debug(): void { /* noop */ }
  info(): void { /* noop */ }
  warn(): void { /* noop */ }
  error(): void { /* noop */ }
  child(): RippleLogger { return this; }
}
