/**
 * Redis Cache Client for C.CacheController — Deno-native driver (`jsr:@db/redis`).
 *
 * Uses direct Redis commands instead of Lua EVAL for the hot path
 * (read/write/exists/remove). Only the atomic increment-with-limit path
 * still uses EVAL because it needs server-side atomicity.
 */

import type { Redis } from '@db/redis';
import { C } from '@panth977/cache';
import type { F } from '@panth977/functions';

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

export class RedisCacheClient extends C.CacheController {
  constructor(
    protected redis: {
      client: Redis;
      decode: <T>(val: string) => T;
      encode: <T>(val: T) => string;
    },
  ) {
    super();
  }
  get client(): Redis {
    return this.redis.client;
  }

  override async existsKey(_c: F.Context, opt: { key: C.KEY }): Promise<boolean> {
    const n = await this.redis.client.exists(opt.key.toString());
    return n > 0;
  }

  override async existsHashFields(
    _c: F.Context,
    opt: { key: C.KEY; fields: C.KEY[] | C.AllFields },
  ): Promise<Record<string, boolean>> {
    const key = opt.key.toString();
    if (opt.fields === '*') {
      const flat = await this.redis.client.hgetall(key);
      const out: Record<string, boolean> = {};
      for (let i = 0; i < flat.length; i += 2) out[flat[i]] = true;
      return out;
    }
    const fields = opt.fields.map((x) => x.toString());
    if (!fields.length) return {};
    const results = await Promise.all(fields.map((f) => this.redis.client.hexists(key, f)));
    const out: Record<string, boolean> = {};
    for (let i = 0; i < fields.length; i++) out[fields[i]] = results[i] > 0;
    return out;
  }

  override async readKey<T>(_c: F.Context, opt: { key: C.KEY }): Promise<T | undefined> {
    const v = await this.redis.client.get(opt.key.toString());
    if (v == null) return undefined;
    return this.redis.decode<T>(v);
  }

  override async readHashFields<T extends Record<string, unknown>>(
    _c: F.Context,
    opt: { key: C.KEY; fields: C.KEY[] | C.AllFields },
  ): Promise<Partial<T>> {
    const key = opt.key.toString();
    if (opt.fields === '*') {
      const flat = await this.redis.client.hgetall(key);
      const ret: Partial<T> = {};
      for (let i = 0; i < flat.length; i += 2) {
        (ret as any)[flat[i]] = this.redis.decode(flat[i + 1]);
      }
      return ret;
    }
    const fields = opt.fields.map((x) => x.toString());
    if (!fields.length) return {};
    const vals = await this.redis.client.hmget(key, ...fields);
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
    const key = opt.key.toString();
    const value = this.redis.encode(opt.value);
    if (opt.expiry > 0) {
      await this.redis.client.set(key, value, { ex: opt.expiry });
    } else {
      await this.redis.client.set(key, value);
    }
  }


  override async writeHashFields<T extends Record<string, unknown>>(
    _c: F.Context,
    opt: { expiry: number; key: C.KEY; value: T },
  ): Promise<void> {
    const key = opt.key.toString();
    const fields = Object.keys(opt.value);
    if (!fields.length) return;
    const type = await this.redis.client.type(key);
    if (type !== 'hash' && type !== 'none') {
      await this.redis.client.del(key);
    }
    const payload: Record<string, string> = {};
    for (const f of fields) payload[f] = this.redis.encode(opt.value[f]);
    await this.redis.client.hset(key, payload);
    if (type !== 'hash' && opt.expiry > 0) {
      await this.redis.client.expire(key, opt.expiry);
    }
  }

  override async removeKey(_c: F.Context, opt: { key: C.KEY }): Promise<void> {
    await this.redis.client.del(opt.key.toString());
  }

  override async removeHashFields(
    _c: F.Context,
    opt: { key: C.KEY; fields: C.KEY[] | C.AllFields },
  ): Promise<void> {
    const key = opt.key.toString();
    if (opt.fields === '*') {
      await this.redis.client.del(key);
      return;
    }
    const fields = opt.fields.map((x) => x.toString());
    if (!fields.length) return;
    await this.redis.client.hdel(key, ...fields);
  }

  override async incrementKey(
    _c: F.Context,
    opt: { expiry: number; key: C.KEY; incrBy: number; maxLimit: number },
  ): Promise<{ allowed: boolean; value: number }> {
    const key = opt.key.toString();
    const result = (await this.redis.client.eval(
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
    const key = opt.key.toString();
    const field = opt.field.toString();
    const result = (await this.redis.client.eval(
      hincrementScript,
      [key],
      [field, String(opt.incrBy), String(opt.maxLimit ?? 0), String(opt.expiry)],
    )) as [number, number];
    return { allowed: !!result[0], value: result[1] };
  }

  override dispose(): void {
    this.redis.client.close();
  }
}
