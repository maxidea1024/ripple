// ---------------------------------------------------------------------------
// @gatrix/ripple ??Registry Tests
// ---------------------------------------------------------------------------

import { RefreshableRegistry } from '../src/registry';
import { Refreshable } from '../src/types';

function makeRefreshable(
  key: string,
  opts?: Partial<Refreshable>,
): Refreshable {
  return {
    key,
    refresh: jest.fn().mockResolvedValue(undefined),
    ...opts,
  };
}

describe('RefreshableRegistry', () => {
  let registry: RefreshableRegistry;

  beforeEach(() => {
    registry = new RefreshableRegistry();
  });

  // -----------------------------------------------------------------------
  // register / get
  // -----------------------------------------------------------------------

  describe('register & get', () => {
    it('should register and retrieve a refreshable by key', () => {
      const r = makeRefreshable('item-table');
      registry.register(r);

      expect(registry.get('item-table')).toBe(r);
      expect(registry.size).toBe(1);
    });

    it('should return undefined for unknown keys', () => {
      expect(registry.get('nope')).toBeUndefined();
    });

    it('should throw on duplicate key registration', () => {
      registry.register(makeRefreshable('dup'));
      expect(() => registry.register(makeRefreshable('dup'))).toThrow(
        'Duplicate refreshable key',
      );
    });

    it('should register multiple refreshables', () => {
      registry.register(makeRefreshable('a'));
      registry.register(makeRefreshable('b'));
      registry.register(makeRefreshable('c'));

      expect(registry.size).toBe(3);
      expect(registry.keys()).toEqual(expect.arrayContaining(['a', 'b', 'c']));
    });
  });

  // -----------------------------------------------------------------------
  // getAll / keys
  // -----------------------------------------------------------------------

  describe('getAll & keys', () => {
    it('should return all registered refreshables', () => {
      registry.register(makeRefreshable('x'));
      registry.register(makeRefreshable('y'));

      const all = registry.getAll();
      expect(all).toHaveLength(2);
      expect(all.map((r) => r.key).sort()).toEqual(['x', 'y']);
    });

    it('should return empty arrays when nothing is registered', () => {
      expect(registry.getAll()).toEqual([]);
      expect(registry.keys()).toEqual([]);
    });
  });

  // -----------------------------------------------------------------------
  // match (wildcard)
  // -----------------------------------------------------------------------

  describe('match (glob pattern)', () => {
    beforeEach(() => {
      registry.register(makeRefreshable('event/summer'));
      registry.register(makeRefreshable('event/halloween'));
      registry.register(makeRefreshable('localization/ko'));
      registry.register(makeRefreshable('localization/en'));
      registry.register(makeRefreshable('item-table'));
      registry.register(makeRefreshable('shop-config'));
    });

    it('should match wildcard pattern', () => {
      const matched = registry.match('event/*');
      expect(matched.map((r) => r.key).sort()).toEqual([
        'event/halloween',
        'event/summer',
      ]);
    });

    it('should match localization wildcard', () => {
      const matched = registry.match('localization/*');
      expect(matched.map((r) => r.key).sort()).toEqual([
        'localization/en',
        'localization/ko',
      ]);
    });

    it('should match exact key', () => {
      const matched = registry.match('item-table');
      expect(matched).toHaveLength(1);
      expect(matched[0].key).toBe('item-table');
    });

    it('should match all with *', () => {
      // minimatch '*' matches strings without slashes
      const matched = registry.match('*');
      const keys = matched.map((r) => r.key);
      expect(keys).toContain('item-table');
      expect(keys).toContain('shop-config');
    });

    it('should match all with **', () => {
      const matched = registry.match('**');
      expect(matched).toHaveLength(6);
    });

    it('should return empty for unmatched pattern', () => {
      const matched = registry.match('nonexistent/*');
      expect(matched).toEqual([]);
    });

    it('should match comma-separated exact keys', () => {
      const matched = registry.match('item-table,shop-config');
      expect(matched.map((r) => r.key).sort()).toEqual([
        'item-table',
        'shop-config',
      ]);
    });

    it('should match comma-separated with wildcards', () => {
      const matched = registry.match('event/*,localization/ko');
      expect(matched.map((r) => r.key).sort()).toEqual([
        'event/halloween',
        'event/summer',
        'localization/ko',
      ]);
    });

    it('should deduplicate overlapping comma patterns', () => {
      const matched = registry.match('event/*,event/summer');
      expect(matched.map((r) => r.key).sort()).toEqual([
        'event/halloween',
        'event/summer',
      ]);
    });
  });

  // -----------------------------------------------------------------------
  // getDependents
  // -----------------------------------------------------------------------

  describe('getDependents', () => {
    it('should find dependents of a key', () => {
      registry.register(makeRefreshable('shop-config'));
      registry.register(
        makeRefreshable('event/summer', { dependsOn: ['shop-config'] }),
      );
      registry.register(
        makeRefreshable('event/halloween', { dependsOn: ['shop-config'] }),
      );
      registry.register(makeRefreshable('item-table'));

      const deps = registry.getDependents('shop-config');
      expect(deps.map((r) => r.key).sort()).toEqual([
        'event/halloween',
        'event/summer',
      ]);
    });

    it('should return empty when no dependents exist', () => {
      registry.register(makeRefreshable('standalone'));
      expect(registry.getDependents('standalone')).toEqual([]);
    });
  });

  // -----------------------------------------------------------------------
  // validateDependencies
  // -----------------------------------------------------------------------

  describe('validateDependencies', () => {
    it('should pass for valid dependency graph', () => {
      registry.register(makeRefreshable('a'));
      registry.register(makeRefreshable('b', { dependsOn: ['a'] }));
      registry.register(makeRefreshable('c', { dependsOn: ['b'] }));

      expect(() => registry.validateDependencies()).not.toThrow();
    });

    it('should pass when no dependencies exist', () => {
      registry.register(makeRefreshable('a'));
      registry.register(makeRefreshable('b'));

      expect(() => registry.validateDependencies()).not.toThrow();
    });

    it('should throw for unknown dependency reference', () => {
      registry.register(
        makeRefreshable('a', { dependsOn: ['nonexistent'] }),
      );

      expect(() => registry.validateDependencies()).toThrow(
        'depends on unknown key',
      );
    });

    it('should throw for circular dependency (direct)', () => {
      registry.register(makeRefreshable('a', { dependsOn: ['b'] }));
      registry.register(makeRefreshable('b', { dependsOn: ['a'] }));

      expect(() => registry.validateDependencies()).toThrow(
        'Circular dependency',
      );
    });

    it('should throw for circular dependency (transitive)', () => {
      registry.register(makeRefreshable('a', { dependsOn: ['c'] }));
      registry.register(makeRefreshable('b', { dependsOn: ['a'] }));
      registry.register(makeRefreshable('c', { dependsOn: ['b'] }));

      expect(() => registry.validateDependencies()).toThrow(
        'Circular dependency',
      );
    });
  });

  // -----------------------------------------------------------------------
  // topologicalSort
  // -----------------------------------------------------------------------

  describe('topologicalSort', () => {
    it('should return keys in dependency order', () => {
      registry.register(makeRefreshable('shop-config'));
      registry.register(
        makeRefreshable('event/summer', { dependsOn: ['shop-config'] }),
      );
      registry.register(
        makeRefreshable('item-table', { dependsOn: ['shop-config'] }),
      );

      const sorted = registry.topologicalSort();

      const shopIdx = sorted.indexOf('shop-config');
      const eventIdx = sorted.indexOf('event/summer');
      const itemIdx = sorted.indexOf('item-table');

      expect(shopIdx).toBeLessThan(eventIdx);
      expect(shopIdx).toBeLessThan(itemIdx);
    });

    it('should handle complex dependency graph', () => {
      // a -> b -> d
      // a -> c -> d
      registry.register(makeRefreshable('d'));
      registry.register(makeRefreshable('b', { dependsOn: ['d'] }));
      registry.register(makeRefreshable('c', { dependsOn: ['d'] }));
      registry.register(makeRefreshable('a', { dependsOn: ['b', 'c'] }));

      const sorted = registry.topologicalSort();

      expect(sorted.indexOf('d')).toBeLessThan(sorted.indexOf('b'));
      expect(sorted.indexOf('d')).toBeLessThan(sorted.indexOf('c'));
      expect(sorted.indexOf('b')).toBeLessThan(sorted.indexOf('a'));
      expect(sorted.indexOf('c')).toBeLessThan(sorted.indexOf('a'));
    });
  });
});
