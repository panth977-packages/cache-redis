/**
 * Redis Cache Client for C.CacheController — Deno-native driver (`jsr:@db/redis`).
 *
 * Uses direct Redis commands instead of Lua EVAL for the hot path
 * (read/write/exists/remove). Only the atomic increment-with-limit path
 * still uses EVAL because it needs server-side atomicity.
 */

import type { Redis } from "@db/redis";
import { C } from "@panth977/cache";
import type { F } from "@panth977/functions";

export function decode<T>(val: string): T {
  return JSON.parse(val);
}
export function encode<T>(val: T): string {
  return JSON.stringify(val);
}

const incrementScript = `
local key = KEYS[1]
local incrBy = tonumber(ARGV[1])
local maxLimit = tonumber(ARGV[2])
local exp = tonumber(ARGV[3])
local cur = tonumber(redis.call('get', key) or '0')
if maxLimit and maxLimit > 0 and cur + incrBy > maxLimit then
  return {0, cur}
end
cur = redis.call('incrby', key, incrBy)
if exp and exp > 0 then
  redis.call('expire', key, exp)
end
return {1, cur}
`;

const hincrementScript = `
local key = KEYS[1]
local field = ARGV[1]
local incrBy = tonumber(ARGV[2])
local maxLimit = tonumber(ARGV[3])
local exp = tonumber(ARGV[4])
local cur = tonumber(redis.call('hget', key, field) or '0')
if maxLimit and maxLimit > 0 and cur + incrBy > maxLimit then
  return {0, cur}
end
cur = redis.call('hincrby', key, field, incrBy)
if exp and exp > 0 then
  redis.call('expire', key, exp)
end
return {1, cur}
`;

type ReadWaiter = {
  resolve: (v: string | null) => void;
  reject: (e: unknown) => void;
};

export class RedisCacheClient extends C.CacheController {
  private pendingReads = new Map<string, ReadWaiter[]>();
  private flushScheduled = false;

  constructor(
    protected redis: {
      futureClient: Promise<Redis>;
      decode: <T>(val: string) => T;
      encode: <T>(val: T) => string;
    },
  ) {
    super();
  }
  get client(): Promise<Redis> {
    return this.redis.futureClient;
  }

  private scheduleReadFlush(): void {
    if (this.flushScheduled) return;
    this.flushScheduled = true;
    queueMicrotask(() => this.flushReads());
  }

  private async flushReads(): Promise<void> {
    this.flushScheduled = false;
    const pending = this.pendingReads;
    if (pending.size === 0) return;
    this.pendingReads = new Map();
    const keys = [...pending.keys()];
    try {
      const client = await this.redis.futureClient;
      const values = await client.mget(...keys);
      for (let i = 0; i < keys.length; i++) {
        const v = values[i] ?? null;
        for (const w of pending.get(keys[i])!) w.resolve(v);
      }
    } catch (err) {
      for (const waiters of pending.values()) {
        for (const w of waiters) w.reject(err);
      }
    }
  }

  override async existsKey(_c: F.Context, opt: { key: C.KEY }): Promise<boolean> {
    const client = await this.redis.futureClient;
    const n = await client.exists(opt.key.toString());
    return n > 0;
  }

  override async existsHashFields(
    _c: F.Context,
    opt: { key: C.KEY; fields: C.KEY[] | C.AllFields },
  ): Promise<Record<string, boolean>> {
    const client = await this.redis.futureClient;
    const key = opt.key.toString();
    if (opt.fields === "*") {
      const flat = await client.hgetall(key);
      const out: Record<string, boolean> = {};
      for (let i = 0; i < flat.length; i += 2) out[flat[i]] = true;
      return out;
    }
    const fields = opt.fields.map((x) => x.toString());
    if (!fields.length) return {};
    const results = await Promise.all(fields.map((f) => client.hexists(key, f)));
    const out: Record<string, boolean> = {};
    for (let i = 0; i < fields.length; i++) out[fields[i]] = results[i] > 0;
    return out;
  }

  override readKey<T>(_c: F.Context, opt: { key: C.KEY }): Promise<T | undefined> {
    const key = opt.key.toString();
    return new Promise<string | null>((resolve, reject) => {
      let waiters = this.pendingReads.get(key);
      if (!waiters) {
        waiters = [];
        this.pendingReads.set(key, waiters);
      }
      waiters.push({ resolve, reject });
      this.scheduleReadFlush();
    }).then((v) => (v == null ? undefined : this.redis.decode<T>(v)));
  }

  override async readHashFields<T extends Record<string, unknown>>(
    _c: F.Context,
    opt: { key: C.KEY; fields: C.KEY[] | C.AllFields },
  ): Promise<Partial<T>> {
    const client = await this.redis.futureClient;
    const key = opt.key.toString();
    if (opt.fields === "*") {
      const flat = await client.hgetall(key);
      const ret: Partial<T> = {};
      for (let i = 0; i < flat.length; i += 2) {
        (ret as any)[flat[i]] = this.redis.decode(flat[i + 1]);
      }
      return ret;
    }
    const fields = opt.fields.map((x) => x.toString());
    if (!fields.length) return {};
    const vals = await client.hmget(key, ...fields);
    const ret: Partial<T> = {};
    for (let i = 0; i < fields.length; i++) {
      const v = vals[i];
      if (v != null) (ret as any)[fields[i]] = this.redis.decode(v);
    }
    return ret;
  }

  override async writeKey<T>(
    _c: F.Context,
    opt: { expiry: number; key: C.KEY; value: T },
  ): Promise<void> {
    const client = await this.redis.futureClient;
    const key = opt.key.toString();
    const value = this.redis.encode(opt.value);
    if (opt.expiry > 0) {
      await client.set(key, value, { ex: opt.expiry });
    } else {
      await client.set(key, value);
    }
  }

  override async writeHashFields<T extends Record<string, unknown>>(
    _c: F.Context,
    opt: { expiry: number; key: C.KEY; value: T },
  ): Promise<void> {
    const client = await this.redis.futureClient;
    const key = opt.key.toString();
    const fields = Object.keys(opt.value);
    if (!fields.length) return;
    const type = await client.type(key);
    if (type !== "hash" && type !== "none") {
      await client.del(key);
    }
    const payload: Record<string, string> = {};
    for (const f of fields) payload[f] = this.redis.encode(opt.value[f]);
    await client.hset(key, payload);
    if (type !== "hash" && opt.expiry > 0) {
      await client.expire(key, opt.expiry);
    }
  }

  override async removeKey(_c: F.Context, opt: { key: C.KEY }): Promise<void> {
    const client = await this.redis.futureClient;
    await client.del(opt.key.toString());
  }

  override async removeHashFields(
    _c: F.Context,
    opt: { key: C.KEY; fields: C.KEY[] | C.AllFields },
  ): Promise<void> {
    const client = await this.redis.futureClient;
    const key = opt.key.toString();
    if (opt.fields === "*") {
      await client.del(key);
      return;
    }
    const fields = opt.fields.map((x) => x.toString());
    if (!fields.length) return;
    await client.hdel(key, ...fields);
  }

  override async incrementKey(
    _c: F.Context,
    opt: { expiry: number; key: C.KEY; incrBy: number; maxLimit: number },
  ): Promise<{ allowed: boolean; value: number }> {
    const client = await this.redis.futureClient;
    const key = opt.key.toString();
    const result = (await client.eval(
      incrementScript,
      [key],
      [String(opt.incrBy), String(opt.maxLimit ?? 0), String(opt.expiry)],
    )) as [number, number];
    return { allowed: !!result[0], value: result[1] };
  }

  override async incrementHashField(
    _c: F.Context,
    opt: { expiry: number; key: C.KEY; field: C.KEY; incrBy: number; maxLimit: number },
  ): Promise<{ allowed: boolean; value: number }> {
    const client = await this.redis.futureClient;
    const key = opt.key.toString();
    const field = opt.field.toString();
    const result = (await client.eval(
      hincrementScript,
      [key],
      [field, String(opt.incrBy), String(opt.maxLimit ?? 0), String(opt.expiry)],
    )) as [number, number];
    return { allowed: !!result[0], value: result[1] };
  }

  override async dispose(): Promise<void> {
    const client = await this.redis.futureClient;
    client.close();
  }
}
