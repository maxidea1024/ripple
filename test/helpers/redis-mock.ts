// ---------------------------------------------------------------------------
// @gatrix/ripple ??Redis Mock Helper for Tests
// ---------------------------------------------------------------------------

/**
 * Lightweight ioredis mock that simulates the subset of Redis commands
 * used by ripple: SET (NX/EX/PX), GET, DEL, EVAL, XADD, XREADGROUP,
 * XACK, XGROUP, XPENDING, XCLAIM, XLEN.
 *
 * All data is stored in-memory Maps. TTLs are tracked but NOT auto-expired
 * (call `expireNow()` to simulate expiration in tests).
 */
export class RedisMock {
  private store = new Map<string, { value: string; expiresAt?: number }>();
  private streams = new Map<
    string,
    { entries: Array<{ id: string; fields: string[] }>; nextId: number }
  >();
  private groups = new Map<
    string,
    Map<string, { lastId: string; pending: Map<string, string[]> }>
  >();

  // -----------------------------------------------------------------------
  // String commands
  // -----------------------------------------------------------------------

  async set(
    key: string,
    value: string,
    ...args: (string | number)[]
  ): Promise<string | null> {
    const upperArgs = args.map((a) =>
      typeof a === 'string' ? a.toUpperCase() : a,
    );
    const hasNX = upperArgs.includes('NX');
    const hasXX = upperArgs.includes('XX');

    if (hasNX && this.store.has(key)) {
      const entry = this.store.get(key)!;
      if (!entry.expiresAt || entry.expiresAt > Date.now()) {
        return null;
      }
      // Expired ??allow overwrite
      this.store.delete(key);
    }

    if (hasXX && !this.store.has(key)) {
      return null;
    }

    let expiresAt: number | undefined;
    const exIdx = upperArgs.indexOf('EX');
    if (exIdx !== -1) {
      const secs = Number(args[exIdx + 1]);
      expiresAt = Date.now() + secs * 1000;
    }
    const pxIdx = upperArgs.indexOf('PX');
    if (pxIdx !== -1) {
      const ms = Number(args[pxIdx + 1]);
      expiresAt = Date.now() + ms;
    }

    this.store.set(key, { value, expiresAt });
    return 'OK';
  }

  async get(key: string): Promise<string | null> {
    const entry = this.store.get(key);
    if (!entry) return null;
    if (entry.expiresAt && entry.expiresAt <= Date.now()) {
      this.store.delete(key);
      return null;
    }
    return entry.value;
  }

  async del(...keys: string[]): Promise<number> {
    let count = 0;
    for (const key of keys) {
      if (this.store.delete(key)) count++;
    }
    return count;
  }

  async pexpire(key: string, ms: number): Promise<number> {
    const entry = this.store.get(key);
    if (!entry) return 0;
    entry.expiresAt = Date.now() + ms;
    return 1;
  }

  // -----------------------------------------------------------------------
  // Lua eval (simplified ??handles lock release/extend scripts)
  // -----------------------------------------------------------------------

  async eval(
    script: string,
    _numKeys: number,
    ...args: string[]
  ): Promise<number> {
    const key = args[0];
    const expectedValue = args[1];

    // Release script: if get == argv[1] then del
    if (script.includes('del')) {
      const entry = this.store.get(key);
      if (entry && entry.value === expectedValue) {
        this.store.delete(key);
        return 1;
      }
      return 0;
    }

    // Extend script: if get == argv[1] then pexpire argv[2]
    if (script.includes('pexpire')) {
      const entry = this.store.get(key);
      if (entry && entry.value === expectedValue) {
        const ms = Number(args[2]);
        entry.expiresAt = Date.now() + ms;
        return 1;
      }
      return 0;
    }

    return 0;
  }

  // -----------------------------------------------------------------------
  // Stream commands
  // -----------------------------------------------------------------------

  async xadd(
    streamKey: string,
    id: string,
    ...fields: string[]
  ): Promise<string> {
    if (!this.streams.has(streamKey)) {
      this.streams.set(streamKey, { entries: [], nextId: 1 });
    }
    const stream = this.streams.get(streamKey)!;

    // Handle MAXLEN trimming
    let fieldsStart = 0;
    if (fields[0]?.toUpperCase() === 'MAXLEN') {
      // Skip MAXLEN [~] <count>
      fieldsStart = fields[1] === '~' ? 3 : 2;
    }

    const actualFields = fields.slice(fieldsStart);
    const entryId =
      id === '*' ? `${Date.now()}-${stream.nextId++}` : id;
    stream.entries.push({ id: entryId, fields: actualFields });
    return entryId;
  }

  async xgroup(
    subcommand: string,
    streamKey: string,
    groupName: string,
    startId: string,
    ...args: string[]
  ): Promise<string> {
    if (subcommand.toUpperCase() === 'CREATE') {
      if (!this.streams.has(streamKey)) {
        if (args.includes('MKSTREAM')) {
          this.streams.set(streamKey, { entries: [], nextId: 1 });
        } else {
          throw new Error('ERR The STREAM key does not exist');
        }
      }
      if (!this.groups.has(streamKey)) {
        this.groups.set(streamKey, new Map());
      }
      this.groups.get(streamKey)!.set(groupName, {
        lastId: startId === '$' ? '0-0' : startId,
        pending: new Map(),
      });
      return 'OK';
    }
    return 'OK';
  }

  async xreadgroup(
    _group: string,
    groupName: string,
    _consumerName: string,
    ..._args: (string | number)[]
  ): Promise<Array<[string, Array<[string, string[]]>]> | null> {
    // Parse args to find stream key
    const args = _args.map(String);
    const streamsIdx = args.findIndex((a) => a.toUpperCase() === 'STREAMS');
    if (streamsIdx === -1) return null;

    const streamKeys = [];
    const ids = [];
    const remaining = args.slice(streamsIdx + 1);
    const half = remaining.length / 2;
    for (let i = 0; i < half; i++) {
      streamKeys.push(remaining[i]);
      ids.push(remaining[half + i]);
    }

    const results: Array<[string, Array<[string, string[]]>]> = [];

    for (let i = 0; i < streamKeys.length; i++) {
      const streamKey = streamKeys[i];
      const stream = this.streams.get(streamKey);
      if (!stream) continue;

      const groupData = this.groups.get(streamKey)?.get(groupName);
      if (!groupData) continue;

      if (ids[i] === '>') {
        // New messages only
        const newEntries = stream.entries.filter(
          (e) => e.id > groupData.lastId,
        );
        if (newEntries.length > 0) {
          const mapped: Array<[string, string[]]> = newEntries.map((e) => {
            groupData.pending.set(e.id, e.fields);
            return [e.id, e.fields];
          });
          groupData.lastId = newEntries[newEntries.length - 1].id;
          results.push([streamKey, mapped]);
        }
      }
    }

    // Parse BLOCK timeout from args
    const blockIdx = args.findIndex((a) => a.toUpperCase() === 'BLOCK');
    const blockMs = blockIdx !== -1 ? Number(args[blockIdx + 1]) : 0;

    return results.length > 0
      ? results
      : new Promise((resolve) =>
          setTimeout(() => resolve(null), Math.min(blockMs, 50)),
        );
  }

  async xack(
    streamKey: string,
    groupName: string,
    ...ids: string[]
  ): Promise<number> {
    const groupData = this.groups.get(streamKey)?.get(groupName);
    if (!groupData) return 0;

    let count = 0;
    for (const id of ids) {
      if (groupData.pending.delete(id)) count++;
    }
    return count;
  }

  async xpending(
    streamKey: string,
    groupName: string,
    ..._args: (string | number)[]
  ): Promise<Array<[string, string, number, number]>> {
    const groupData = this.groups.get(streamKey)?.get(groupName);
    if (!groupData) return [];

    return [...groupData.pending.keys()].map((id) => [
      id,
      'worker',
      60000, // idle time
      1, // delivery count
    ]);
  }

  async xclaim(
    streamKey: string,
    groupName: string,
    _consumerName: string,
    _minIdleTime: number,
    ...ids: string[]
  ): Promise<Array<[string, string[]]>> {
    const groupData = this.groups.get(streamKey)?.get(groupName);
    if (!groupData) return [];

    const claimed: Array<[string, string[]]> = [];
    for (const id of ids) {
      const fields = groupData.pending.get(id);
      if (fields) {
        claimed.push([id, fields]);
      }
    }
    return claimed;
  }

  async xlen(streamKey: string): Promise<number> {
    return this.streams.get(streamKey)?.entries.length ?? 0;
  }

  // -----------------------------------------------------------------------
  // Utilities for testing
  // -----------------------------------------------------------------------

  /** Simulate key expiration */
  expireKey(key: string): void {
    this.store.delete(key);
  }

  /** Clear all data */
  flushall(): void {
    this.store.clear();
    this.streams.clear();
    this.groups.clear();
  }

  /** Disconnect stub */
  async quit(): Promise<string> {
    return 'OK';
  }

  async disconnect(): Promise<void> {
    // noop
  }

  /** Duplicate stub ??returns self for simplicity */
  duplicate(): RedisMock {
    const dup = new RedisMock();
    dup.store = this.store;
    dup.streams = this.streams;
    dup.groups = this.groups;
    return dup;
  }
}
