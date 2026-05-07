// ---------------------------------------------------------------------------
// @gatrix/ripple ??Refreshable Registry
// ---------------------------------------------------------------------------

import { minimatch } from 'minimatch';
import { Refreshable } from './types';

/**
 * Registry for refreshable handlers.
 *
 * - Stores handlers keyed by their unique `key` string.
 * - Supports glob-style wildcard matching via minimatch.
 * - Validates dependency graphs to detect cycles at startup.
 */
export class RefreshableRegistry {
  private readonly items = new Map<string, Refreshable>();

  /**
   * Register a refreshable handler.
   * @throws if a handler with the same key is already registered.
   */
  register(refreshable: Refreshable): void {
    if (this.items.has(refreshable.key)) {
      throw new Error(
        `[ripple] Duplicate refreshable key: "${refreshable.key}"`,
      );
    }
    this.items.set(refreshable.key, refreshable);
  }

  /** Get a handler by exact key. */
  get(key: string): Refreshable | undefined {
    return this.items.get(key);
  }

  /** Get all registered handlers. */
  getAll(): Refreshable[] {
    return [...this.items.values()];
  }

  /** Get all registered keys. */
  keys(): string[] {
    return [...this.items.keys()];
  }

  /** Number of registered handlers. */
  get size(): number {
    return this.items.size;
  }

  /**
   * Find all handlers whose key matches the given glob pattern.
   * Uses minimatch for matching. Supports comma-separated patterns.
   *
   * @example
   *   registry.match('event/*')                  // all under event/
   *   registry.match('item-table,shop-config')   // exact two keys
   *   registry.match('event/*,localization/*')   // two wildcards
   *   registry.match('**')                       // all handlers
   */
  match(pattern: string): Refreshable[] {
    const patterns = pattern.split(',').map((p) => p.trim()).filter(Boolean);
    const all = [...this.items.values()];

    if (patterns.length === 1) {
      return all.filter((r) => minimatch(r.key, patterns[0]));
    }

    // Union of multiple patterns (deduplicated)
    const matched = new Map<string, Refreshable>();
    for (const p of patterns) {
      for (const r of all) {
        if (!matched.has(r.key) && minimatch(r.key, p)) {
          matched.set(r.key, r);
        }
      }
    }
    return [...matched.values()];
  }

  /**
   * Get handlers that depend on the given key.
   * (i.e. handlers whose `dependsOn` includes `key`)
   */
  getDependents(key: string): Refreshable[] {
    return [...this.items.values()].filter(
      (r) => r.dependsOn?.includes(key) ?? false,
    );
  }

  /**
   * Given a set of initially matched keys, expand with all transitive
   * dependents (cascade) and return them in topological order.
   *
   * Example: if item-table is refreshed with cascade=true, and
   * shop-config depends on item-table, and price-calc depends on
   * shop-config, the result is:
   *   [item-table, shop-config, price-calc]
   */
  collectCascade(initialKeys: string[]): Refreshable[] {
    const visited = new Set<string>(initialKeys);
    const queue = [...initialKeys];

    // BFS to find all transitive dependents
    while (queue.length > 0) {
      const key = queue.shift()!;
      for (const dep of this.getDependents(key)) {
        if (!visited.has(dep.key)) {
          visited.add(dep.key);
          queue.push(dep.key);
        }
      }
    }

    // Return in topological order (subset of full sort)
    const fullOrder = this.topologicalSort();
    return fullOrder
      .filter((key) => visited.has(key))
      .map((key) => this.items.get(key)!)
      .filter(Boolean);
  }

  /**
   * Validate the dependency graph:
   * 1. All `dependsOn` references must point to registered keys.
   * 2. No circular dependencies.
   *
   * @throws on invalid references or cycles.
   */
  validateDependencies(): void {
    // Check all references exist
    for (const item of this.items.values()) {
      if (!item.dependsOn) continue;
      for (const dep of item.dependsOn) {
        if (!this.items.has(dep)) {
          throw new Error(
            `[ripple] Refreshable "${item.key}" depends on unknown key "${dep}"`,
          );
        }
      }
    }

    // Detect cycles via topological sort (Kahn's algorithm)
    this.topologicalSort();
  }

  /**
   * Print the dependency graph to the provided logger.
   * Intended to be called only when debug logging is enabled.
   *
   * Output example:
   *   [ripple] Dependency Graph (4 refreshables):
   *     shop-config
   *       ??event/summer
   *       ??event/halloween
   *     item-table (no dependents)
   */
  printDependencyGraph(logger: { debug: (...args: any[]) => void }): void {
    const sorted = this.topologicalSort();
    const lines: string[] = [
      `Dependency Graph (${this.items.size} refreshables):`,
    ];

    for (const key of sorted) {
      const item = this.items.get(key)!;
      const depsLabel = item.dependsOn?.length
        ? ` [depends on: ${item.dependsOn.join(', ')}]`
        : '';
      const dependents = this.getDependents(key);

      lines.push(`  ${key}${depsLabel}`);
      if (dependents.length > 0) {
        for (const d of dependents) {
          lines.push(`    ??${d.key}`);
        }
      } else {
        lines.push('    (no dependents)');
      }
    }

    logger.debug(lines.join('\n'));
  }

  /**
   * Return keys in dependency-safe execution order (topological sort).
   * Dependencies come before their dependents.
   *
   * @throws on circular dependencies.
   */
  topologicalSort(): string[] {
    // Build adjacency and in-degree
    const inDegree = new Map<string, number>();
    const edges = new Map<string, string[]>(); // dep ??dependents

    for (const key of this.items.keys()) {
      inDegree.set(key, 0);
      if (!edges.has(key)) edges.set(key, []);
    }

    for (const item of this.items.values()) {
      if (!item.dependsOn) continue;
      for (const dep of item.dependsOn) {
        edges.get(dep)!.push(item.key);
        inDegree.set(item.key, (inDegree.get(item.key) ?? 0) + 1);
      }
    }

    // Kahn's algorithm
    const queue: string[] = [];
    for (const [key, deg] of inDegree) {
      if (deg === 0) queue.push(key);
    }

    const sorted: string[] = [];
    while (queue.length > 0) {
      const current = queue.shift()!;
      sorted.push(current);

      for (const dependent of edges.get(current) ?? []) {
        const newDeg = (inDegree.get(dependent) ?? 1) - 1;
        inDegree.set(dependent, newDeg);
        if (newDeg === 0) queue.push(dependent);
      }
    }

    if (sorted.length !== this.items.size) {
      // Find the cycle for a useful error message
      const remaining = [...this.items.keys()].filter(
        (k) => !sorted.includes(k),
      );
      throw new Error(
        `[ripple] Circular dependency detected among: ${remaining.join(', ')}`,
      );
    }

    return sorted;
  }
}
