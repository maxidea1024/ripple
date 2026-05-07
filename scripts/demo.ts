#!/usr/bin/env ts-node
/**
 * @gatrix/ripple - Interactive Console Demo
 *
 * Usage:
 *   npx ts-node scripts/demo.ts --port 6381
 *
 * Commands:
 *   refresh <pattern>       Publish refresh event
 *   fail <key>              Make a handler throw errors
 *   slow <key> <ms>         Add artificial delay to a handler
 *   heal <key>              Restore handler to normal
 *   status                  Show handler states (fail/slow/normal)
 *   list                    Show registered handlers
 *   help                    Show all commands
 *   quit                    Shutdown
 */

import * as readline from 'readline';
import { createRipple } from '../src/orchestrator';
import { createConsoleLoggerFactory } from '../src/logger';
import { OrchestratorConfig, RefreshContext } from '../src/types';

// ---------------------------------------------------------------------------
// CLI argument parsing
// ---------------------------------------------------------------------------

function parseArgs(): { host: string; port: number; serverId: string } {
  const args = process.argv.slice(2);
  const opts = { host: 'localhost', port: 46379, serverId: `demo-${process.pid}` };

  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--host' && args[i + 1]) opts.host = args[++i];
    if (args[i] === '--port' && args[i + 1]) opts.port = Number(args[++i]);
    if (args[i] === '--id' && args[i + 1]) opts.serverId = args[++i];
  }

  return opts;
}

// ---------------------------------------------------------------------------
// Handler state simulation
// ---------------------------------------------------------------------------

interface HandlerState {
  mode: 'normal' | 'fail' | 'slow';
  baseDelayMs: number;
  slowDelayMs: number;
  failMessage: string;
  callCount: number;
}

const handlerStates = new Map<string, HandlerState>();

function getState(key: string): HandlerState {
  return handlerStates.get(key)!;
}

function createSimHandler(key: string, baseDelayMs: number) {
  handlerStates.set(key, {
    mode: 'normal',
    baseDelayMs,
    slowDelayMs: 0,
    failMessage: '',
    callCount: 0,
  });

  return async (ctx: RefreshContext) => {
    const state = getState(key);
    state.callCount++;
    const count = state.callCount;

    const delay = state.mode === 'slow' ? state.slowDelayMs : state.baseDelayMs;
    const start = Date.now();

    // Simulate work
    await new Promise((r) => setTimeout(r, delay));
    const elapsed = Date.now() - start;

    if (state.mode === 'fail') {
      const msg = state.failMessage || `Simulated failure in ${key}`;
      console.log(
        `    [${key}] FAIL trigger=${ctx.trigger} elapsed=${elapsed}ms (call #${count}) error="${msg}"`,
      );
      throw new Error(msg);
    }

    const modeTag = state.mode === 'slow' ? ' (SLOW)' : '';
    console.log(
      `    [${key}] OK trigger=${ctx.trigger} elapsed=${elapsed}ms (call #${count})${modeTag}`,
    );
  };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const opts = parseArgs();

  console.log('='.repeat(60));
  console.log('  @gatrix/ripple - Interactive Console Demo');
  console.log('='.repeat(60));
  console.log(`  Redis:     ${opts.host}:${opts.port}`);
  console.log(`  Server ID: ${opts.serverId}`);
  console.log('');

  const config: OrchestratorConfig = {
    serverId: opts.serverId,
    redis: { host: opts.host, port: opts.port },
    logLevel: 'info',
    defaultTimeoutMs: 10000,
    stream: { key: 'ripple-demo-stream' },
    bootstrap: { failFast: false },
    retry: { maxRetries: 2, retryDelayMs: 500 },
  };

  const loggerFactory = createConsoleLoggerFactory('info');
  const ripple = createRipple(config, loggerFactory);

  // Register sample handlers
  ripple
    .register({
      key: 'item-table',
      refresh: createSimHandler('item-table', 300),
    })
    .register({
      key: 'shop-config',
      refresh: createSimHandler('shop-config', 200),
      dependsOn: ['item-table'],
    })
    .register({
      key: 'event/summer',
      refresh: createSimHandler('event/summer', 100),
      debounceMs: 2000,
    })
    .register({
      key: 'event/halloween',
      refresh: createSimHandler('event/halloween', 100),
    })
    .register({
      key: 'localization/ko',
      refresh: createSimHandler('localization/ko', 150),
    })
    .register({
      key: 'localization/en',
      refresh: createSimHandler('localization/en', 150),
    });

  // Start (bootstrap + consumer)
  console.log('\n--- Bootstrap ---\n');
  try {
    const result = await ripple.start();
    console.log(
      `\n  Bootstrap: ${result.successCount}/${result.totalCount} ok (${result.durationMs}ms)\n`,
    );
  } catch (err: any) {
    console.error(`\n  Bootstrap failed: ${err.message}\n`);
    process.exit(1);
  }

  printHelp();

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: 'ripple> ',
  });

  rl.prompt();

  rl.on('line', async (line: string) => {
    const trimmed = line.trim();
    if (!trimmed) {
      rl.prompt();
      return;
    }

    const [cmd, ...rest] = trimmed.split(/\s+/);
    const arg1 = rest[0] || '';
    const arg2 = rest[1] || '';

    switch (cmd.toLowerCase()) {
      // -------------------------------------------------------------------
      // refresh
      // -------------------------------------------------------------------
      case 'refresh':
      case 'r': {
        // Strip spaces around commas so "item-table, shop-config" works
        const pattern = rest.join(' ').replace(/\s*,\s*/g, ',').trim();
        if (!pattern) {
          console.log('  Usage: refresh <pattern>');
          console.log('  Examples: refresh event/*   refresh item-table, shop-config   refresh **');
          break;
        }

        // Detect common mistake: "refresh fail ..." instead of "fail ..."
        const firstWord = rest[0]?.toLowerCase();
        if (firstWord === 'fail' || firstWord === 'slow' || firstWord === 'heal') {
          console.log(`  "${firstWord}" is a separate command, not a refresh option.`);
          console.log(`  Try: ${rest.join(' ')}`);
          break;
        }

        const matched = ripple.registry.match(pattern);
        if (matched.length === 0) {
          console.log(`  No handlers match pattern "${pattern}"`);
          break;
        }

        console.log(
          `  >> Publishing: pattern="${pattern}" -> [${matched.map((r) => r.key).join(', ')}]`,
        );

        try {
          const publisher = (ripple as any).publisher;
          if (publisher) {
            const event = publisher.constructor.createEvent
              ? publisher.constructor.createEvent(pattern, 'console-demo')
              : { requestId: `demo-${Date.now()}`, pattern, triggeredBy: 'console-demo', createdAt: Date.now() };
            await publisher.publish(event);
            console.log(`  >> Published (requestId=${event.requestId})`);
          }
        } catch (err: any) {
          console.error(`  Publish failed: ${err.message}`);
        }

        // Wait for consumer
        const maxWait = Math.max(...matched.map((r) => {
          const s = handlerStates.get(r.key);
          return s?.mode === 'slow' ? s.slowDelayMs : (s?.baseDelayMs || 200);
        }));
        await new Promise((r) => setTimeout(r, maxWait + 1000));
        break;
      }

      // -------------------------------------------------------------------
      // fail - make a handler throw errors
      // -------------------------------------------------------------------
      case 'fail': {
        if (!arg1) {
          console.log('  Usage: fail <key> [message]');
          console.log('  Example: fail item-table "DB connection refused"');
          break;
        }

        const state = handlerStates.get(arg1);
        if (!state) {
          console.log(`  Unknown handler: "${arg1}"`);
          console.log(`  Available: ${[...handlerStates.keys()].join(', ')}`);
          break;
        }

        state.mode = 'fail';
        state.failMessage = rest.slice(1).join(' ') || `Simulated failure in ${arg1}`;
        console.log(`  [!] "${arg1}" will now FAIL with: "${state.failMessage}"`);
        console.log(`      Retry: 2 retries, 500ms base delay, exponential backoff`);
        console.log(`  --> Now run: refresh ${arg1}`);
        break;
      }

      // -------------------------------------------------------------------
      // slow - add artificial delay
      // -------------------------------------------------------------------
      case 'slow': {
        if (!arg1 || !arg2) {
          console.log('  Usage: slow <key> <ms>');
          console.log('  Example: slow item-table 8000    (8 seconds, will test timeout at 10s)');
          console.log('           slow item-table 15000   (15 seconds, WILL timeout)');
          break;
        }

        const state = handlerStates.get(arg1);
        if (!state) {
          console.log(`  Unknown handler: "${arg1}"`);
          console.log(`  Available: ${[...handlerStates.keys()].join(', ')}`);
          break;
        }

        const ms = Number(arg2);
        if (isNaN(ms) || ms <= 0) {
          console.log(`  Invalid delay: "${arg2}". Must be a positive number.`);
          break;
        }

        state.mode = 'slow';
        state.slowDelayMs = ms;
        const willTimeout = ms > (config.defaultTimeoutMs || 10000);
        console.log(`  [!] "${arg1}" delay set to ${ms}ms (base: ${state.baseDelayMs}ms)`);
        if (willTimeout) {
          console.log(`      WARNING: Exceeds timeout (${config.defaultTimeoutMs}ms). Will be killed.`);
        }
        console.log(`  --> Now run: refresh ${arg1}`);
        break;
      }

      // -------------------------------------------------------------------
      // heal - restore normal
      // -------------------------------------------------------------------
      case 'heal': {
        if (!arg1) {
          console.log('  Usage: heal <key>       Restore single handler');
          console.log('         heal all         Restore all handlers');
          break;
        }

        if (arg1 === 'all') {
          for (const [, s] of handlerStates) {
            s.mode = 'normal';
            s.slowDelayMs = 0;
            s.failMessage = '';
          }
          console.log('  [OK] All handlers restored to normal');
        } else {
          const state = handlerStates.get(arg1);
          if (!state) {
            console.log(`  Unknown handler: "${arg1}"`);
            break;
          }
          state.mode = 'normal';
          state.slowDelayMs = 0;
          state.failMessage = '';
          console.log(`  [OK] "${arg1}" restored to normal (${state.baseDelayMs}ms)`);
        }
        break;
      }

      // -------------------------------------------------------------------
      // status - show handler states
      // -------------------------------------------------------------------
      case 'status':
      case 'st': {
        console.log(`\n  Handler Status:\n`);
        console.log(
          '  ' +
            ['Key', 'Mode', 'Delay', 'Calls', 'Detail']
              .map((h, i) => h.padEnd(i === 4 ? 30 : 20))
              .join(''),
        );
        console.log('  ' + '-'.repeat(110));
        for (const [key, s] of handlerStates) {
          const delay = s.mode === 'slow' ? `${s.slowDelayMs}ms` : `${s.baseDelayMs}ms`;
          let detail = '-';
          if (s.mode === 'fail') detail = s.failMessage;
          if (s.mode === 'slow') {
            const willTimeout = s.slowDelayMs > (config.defaultTimeoutMs || 10000);
            detail = willTimeout ? 'WILL TIMEOUT' : 'slow but within limit';
          }
          const mode = s.mode === 'normal' ? 'normal' : s.mode.toUpperCase();
          console.log(
            '  ' +
              [key, mode, delay, String(s.callCount), detail]
                .map((v, i) => v.padEnd(i === 4 ? 30 : 20))
                .join(''),
          );
        }
        console.log('');
        break;
      }

      // -------------------------------------------------------------------
      // list
      // -------------------------------------------------------------------
      case 'list':
      case 'ls': {
        const all = ripple.registry.getAll();
        console.log(`\n  Registered handlers (${all.length}):\n`);
        console.log(
          '  ' +
            ['Key', 'DependsOn', 'DebounceMs', 'TimeoutMs']
              .map((h) => h.padEnd(22))
              .join(''),
        );
        console.log('  ' + '-'.repeat(88));
        for (const r of all) {
          const deps = r.dependsOn?.join(', ') || '-';
          const debounce = r.debounceMs ? String(r.debounceMs) : '-';
          const timeout = r.timeoutMs ? String(r.timeoutMs) : 'default';
          console.log(
            '  ' +
              [r.key, deps, debounce, timeout]
                .map((v) => v.padEnd(22))
                .join(''),
          );
        }
        console.log('');
        break;
      }

      // -------------------------------------------------------------------
      // help
      // -------------------------------------------------------------------
      case 'help':
      case 'h':
      case '?': {
        printHelp();
        break;
      }

      // -------------------------------------------------------------------
      // quit
      // -------------------------------------------------------------------
      case 'quit':
      case 'exit':
      case 'q': {
        console.log('\n  Shutting down...');
        await ripple.shutdown();
        console.log('  Done.');
        rl.close();
        process.exit(0);
        break;
      }

      default:
        console.log(`  Unknown command: "${cmd}". Type "help" for available commands.`);
    }

    rl.prompt();
  });

  rl.on('close', async () => {
    await ripple.shutdown();
    process.exit(0);
  });
}

function printHelp() {
  console.log('-'.repeat(60));
  console.log('  Commands:');
  console.log('');
  console.log('  Refresh:');
  console.log('    refresh <pattern>           Publish refresh event');
  console.log('      refresh event/*           All event handlers');
  console.log('      refresh item-table,shop-config   Multiple keys');
  console.log('      refresh **                Everything');
  console.log('');
  console.log('  Simulation:');
  console.log('    fail <key> [message]        Make handler throw errors');
  console.log('    slow <key> <ms>             Set artificial delay');
  console.log('    heal <key|all>              Restore to normal');
  console.log('    status                      Show handler states');
  console.log('');
  console.log('  Info:');
  console.log('    list                        Show registered handlers');
  console.log('    help                        Show this help');
  console.log('    quit                        Shutdown and exit');
  console.log('-'.repeat(60));
  console.log('');
  console.log('  Try this sequence:');
  console.log('    1. refresh item-table              (normal)');
  console.log('    2. fail item-table DB timeout      (simulate failure)');
  console.log('    3. refresh item-table              (watch retries)');
  console.log('    4. heal item-table                 (restore)');
  console.log('    5. slow shop-config 15000          (exceed 10s timeout)');
  console.log('    6. refresh shop-config             (watch timeout kill)');
  console.log('    7. heal all                        (restore all)');
  console.log('');
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
