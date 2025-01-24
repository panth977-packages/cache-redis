/**
 * Redis Cache Client for CACHE.CacheController
 *
 * @module
 *
 * @example
 * ```ts
 * import { CACHE } from "@panth977/cache";
 * import { RedisCacheClient } from "@panth977/cache-redis";
 * import * as redis from "redis";
 *
 * const cache = new CACHE.CacheController({
 *   client: new RedisCacheClient(redis.createClient(), {
 *     decode: JSON.parse,
 *     encode: JSON.stringify,
 *     delayInMs: 10,
 *     label: "Redis",
 *   }),
 *   allowed: { "*": true, increment: false },
 *   defaultExpiry: 3600,
 *   log: false,
 *   prefix: 'Dev',
 *   separator: ':',
 * });
 *
 * cache.[API]
 * ```
 */

import type {
  RedisClientType,
  RedisDefaultModules,
  RedisFunctions,
  RedisModules,
  RedisScripts,
} from "redis";
import { CACHE } from "@panth977/cache";
import type { FUNCTIONS } from "@panth977/functions";
import { TOOLS } from "@panth977/tools";

function time() {
  const start = Date.now();
  return function () {
    return Date.now() - start;
  };
}
/**
 * Function to decode the data from redis
 * @param val
 * @returns
 */
export function decode<T>(val: string): T {
  return JSON.parse(val);
}
/**
 * Function to encode the data to redis
 * @param val
 * @returns
 */
export function encode<T>(val: T): string {
  return JSON.stringify(val);
}

/**
 * Scripts used for queering redis
 */
export const luaScripts: {
  readonly exists: string;
  readonly read: string;
  readonly write: string;
  readonly remove: string;
  readonly increment: string;
} = {
  exists: `
local result = {}
local null = cjson.decode('null')
for i, key in ipairs(KEYS) do
  local fields = cjson.decode(ARGV[i])
  local keyType = redis.call('type', key).ok
  if keyType == 'string' and fields == null then
    table.insert(result, 1)
  elseif keyType == 'hash' and fields == '*' then
    local hashResult = {}
    for _, field in ipairs(redis.call('hkeys', KEYS[1])) do
      table.insert(hashResult, field)
      table.insert(hashResult, 1)
    end
    table.insert(result, hashResult)
  elseif keyType == 'hash' and fields ~= null and fields ~= '*' then
    local hashResult = {}
    for _, field in ipairs(fields) do
      table.insert(hashResult, field)
      table.insert(hashResult, redis.call('hexists', key, field))
    end
    table.insert(result, hashResult)
  else
    table.insert(result, 0)
  end
end
return result
  `,
  read: `
local result = {}
local null = cjson.decode('null')
for i, key in ipairs(KEYS) do
  local fields = cjson.decode(ARGV[i])
  local keyType = redis.call('type', key).ok
  if keyType == 'string' and fields == null then
    table.insert(result, redis.call('get', key))
  elseif keyType == 'hash' and fields == '*' then
    table.insert(result, redis.call('hgetall', key))
  elseif keyType == 'hash' and fields ~= null and fields ~= '*' then
    local hashResult = {}
    for _, field in ipairs(fields) do
      table.insert(hashResult, field)
      table.insert(hashResult, redis.call('hget', key, field))
    end
    table.insert(result, hashResult)
  else
    table.insert(result, null)
  end
end
return result
  `,
  write: `
for i, key in ipairs(KEYS) do
  local pairOfValAndExp = cjson.decode(ARGV[i])
  local value = pairOfValAndExp[1]
  local exp = pairOfValAndExp[2]
  local keyType = redis.call('type', key).ok

  if type(value) == "string" then
    if keyType ~= 'string' and keyType ~= 'none' then 
      redis.call('del', key)
    end
    redis.call('set', key, value)
    if exp > 0 then
      redis.call('expire', key, exp)
    end

  elseif type(value) == "table" then
    if keyType ~= 'hash' and keyType ~= 'none' then 
      redis.call('del', key)
    end
    for field, fieldValue in pairs(value) do
      redis.call('hset', key, field, fieldValue)
    end
    if exp > 0 then 
      redis.call('expire', key, exp)
    end
  end
end
  `,
  remove: `
local null = cjson.decode('null')
for i, key in ipairs(KEYS) do
  local fields = cjson.decode(ARGV[i])
  local keyType = redis.call('type', key).ok
  if keyType == 'string' and fields == null then
    redis.call('del', key)
  elseif keyType == 'hash' and fields == '*' then
    redis.call('del', key)
  elseif keyType == 'hash' and fields ~= null and fields ~= '*' then
    for _, field in ipairs(fields) do
      redis.call('hdel', key, field)
    end
  end
end
return 1
  `,
  increment: `
local key = KEYS[1]
local incrBy = tonumber(ARGV[1])
local maxLimit = tonumber(ARGV[2])
local expirySec = tonumber(ARGV[3])
local currentValue = tonumber(redis.call('GET', key) or '0')
local allowed = 1
if maxLimit and maxLimit > 0 and currentValue + incrBy > maxLimit then
  allowed = 0
end
if allowed == 1 then
  currentValue = redis.call('INCRBY', key, incrBy)
  if expirySec and expirySec > 0 then
    redis.call('EXPIRE', key, expirySec)
  end
end
return {allowed, currentValue}
  `,
};

/**
 * Use this as a client for CACHE.CacheController
 */ export class RedisCacheClient<
  M extends RedisModules = RedisDefaultModules,
  F extends RedisFunctions = Record<string, never>,
  S extends RedisScripts = Record<string, never>
> extends CACHE.AbstractCacheClient {
  readonly client: RedisClientType<M, F, S>;
  readonly existsExe: (
    cmd: [string, string[] | "*" | undefined]
  ) => Promise<boolean | Record<string, boolean> | null>;
  readonly readExe: (
    cmd: [string, string[] | "*" | undefined]
  ) => Promise<string | Record<string, string> | null>;
  readonly writeExe: (
    cmd: [string, string | Record<string, string>, number]
  ) => Promise<void>;
  readonly removeExe: (
    cmd: [string, string[] | "*" | undefined]
  ) => Promise<void>;
  readonly opt: {
    decode: <T>(val: string) => T;
    encode: <T>(val: T) => string;
    delayInMs: number;
    label: string;
  };

  constructor(
    client: RedisClientType<M, F, S>,
    opt_?: {
      label?: string;
      delayInMs?: number;
      decode?: <T>(val: string) => T;
      encode?: <T>(val: T) => string;
    }
  ) {
    const opt = {
      label: opt_?.label ?? "Redis",
      decode: opt_?.decode ?? decode,
      delayInMs: opt_?.delayInMs ?? 0,
      encode: opt_?.encode ?? encode,
    };
    super(opt.label);
    this.client = client;
    this.opt = opt;
    const timeout = opt.delayInMs;
    this.existsExe = TOOLS.CreateBatchProcessor({
      delayInMs: timeout,
      async implementation(cmds) {
        const result = await client.eval(luaScripts.exists, {
          keys: cmds.map((p) => p[0]),
          arguments: cmds.map((p) => JSON.stringify(p[1] || null)),
        });
        const values = [];
        for (const item of result as (string | string[])[]) {
          if (typeof item === "number" || typeof item === "string") {
            values.push(!!+item);
          } else if (Array.isArray(item)) {
            const obj: Record<string, boolean> = {};
            for (let i = 0; i < item.length; i += 2)
              obj[item[i]] = !!+item[i + 1];
            values.push(obj);
          } else {
            values.push(null);
          }
        }
        return values;
      },
    });
    this.readExe = TOOLS.CreateBatchProcessor({
      delayInMs: timeout,
      async implementation(cmds) {
        const result = await client.eval(luaScripts.read, {
          keys: cmds.map((p) => p[0]),
          arguments: cmds.map((p) => JSON.stringify(p[1] || null)),
        });
        const values = [];
        for (const item of result as (string | string[])[]) {
          if (typeof item === "string") {
            values.push(item);
          } else if (Array.isArray(item)) {
            const obj: Record<string, string> = {};
            for (let i = 0; i < item.length; i += 2) obj[item[i]] = item[i + 1];
            values.push(obj);
          } else {
            values.push(null);
          }
        }
        return values;
      },
    });
    this.writeExe = TOOLS.CreateBatchProcessor({
      delayInMs: timeout,
      async implementation(cmds) {
        await client.eval(luaScripts.write, {
          keys: cmds.map((x) => x[0]),
          arguments: cmds
            .map((x) => [x[1], x[2]])
            .map((x) => JSON.stringify(x)),
        });
        return Array(cmds.length);
      },
    });
    this.removeExe = TOOLS.CreateBatchProcessor({
      delayInMs: timeout,
      async implementation(cmds) {
        await client.eval(luaScripts.remove, {
          keys: cmds.map((p) => p[0]),
          arguments: cmds.map((p) => JSON.stringify(p[1] || null)),
        });
        return Array(cmds.length);
      },
    });
  }
  override async existsKey({
    context,
    key,
    log,
  }: {
    context?: FUNCTIONS.Context;
    key: CACHE.KEY;
    log?: boolean;
  }): Promise<boolean> {
    const timer = time();
    let Err = undefined;
    const value = await this.existsExe([key.toString(), undefined]).catch(
      (err) => {
        Err = err ?? null;
        return false;
      }
    );
    if (log) {
      (context ?? console).log(
        `(${timer()} ms) ${this.name}.exists(${key}) ${
          Err === undefined ? `✅` : `❌: ${Err}`
        }`
      );
    }
    if (value === null || typeof value !== "boolean") return false;
    return value;
  }
  override async existsHashFields({
    context,
    fields,
    key,
    log,
  }: {
    context?: FUNCTIONS.Context;
    key: CACHE.KEY;
    fields: CACHE.AllFields | CACHE.KEY[];
    log?: boolean;
  }): Promise<Record<string, boolean>> {
    const timer = time();
    let Err = undefined;
    const value = await this.existsExe([
      key.toString(),
      fields === "*" ? "*" : fields.map((x) => x.toString()),
    ]).catch((err) => {
      Err = err ?? null;
      return {};
    });
    if (log) {
      (context ?? console).log(
        `(${timer()} ms) ${this.name}.exists(${key}, ${
          fields === "*" ? "*" : `[${fields}]`
        }) ${Err === undefined ? `✅` : `❌: ${Err}`}`
      );
    }
    if (value === null || typeof value === "boolean") return {};
    return value;
  }

  override async readKey<T>({
    context,
    key,
    log,
  }: {
    context?: FUNCTIONS.Context;
    key: CACHE.KEY;
    log?: boolean;
  }): Promise<T | undefined> {
    const timer = time();
    let Err = undefined;
    const value = await this.readExe([key.toString(), undefined]).catch(
      (err) => {
        Err = err ?? null;
        return null;
      }
    );
    if (log) {
      (context ?? console).log(
        `(${timer()} ms) ${this.name}.read(${key}) ${
          Err === undefined ? `✅` : `❌: ${Err}`
        }`
      );
    }
    if (value === null || typeof value !== "string") return undefined;
    return this.opt.decode(value);
  }
  override async readHashFields<T extends Record<string, unknown>>({
    context,
    fields,
    key,
    log,
  }: {
    context?: FUNCTIONS.Context;
    key: CACHE.KEY;
    fields: CACHE.AllFields | CACHE.KEY[];
    log?: boolean;
  }): Promise<Partial<T>> {
    const timer = time();
    let Err = undefined;
    const value = await this.readExe([
      key.toString(),
      fields === "*" ? "*" : fields.map((x) => x.toString()),
    ]).catch((err) => {
      Err = err ?? null;
      return {} as Record<string, string>;
    });
    if (log) {
      (context ?? console).log(
        `(${timer()} ms) ${this.name}.read(${key}, ${
          fields === "*" ? "*" : `[${fields}]`
        }) ${Err === undefined ? `✅` : `❌: ${Err}`}`
      );
    }
    if (value === null || typeof value === "string") return {};
    const ret: Partial<T> = {};
    for (const key in value) {
      if (typeof value[key] === "string") {
        (ret as any)[key] = this.opt.decode(value[key]);
      }
    }
    return ret;
  }

  override async writeKey<T>({
    context,
    expire,
    key,
    value,
    log,
  }: {
    context?: FUNCTIONS.Context;
    key: CACHE.KEY;
    value: T | Promise<T>;
    expire: number;
    log?: boolean;
  }): Promise<void> {
    let awaitedValue: T;
    try {
      awaitedValue = await value;
      if (awaitedValue === undefined) return;
    } catch {
      return;
    }
    const timer = time();
    let Err = undefined;
    const val = this.opt.encode(awaitedValue);
    await this.writeExe([key.toString(), val, expire]).catch((err) => {
      Err = err ?? null;
    });
    if (log) {
      (context ?? console).log(
        `(${timer()} ms) ${this.name}.write(${key}) ${
          Err === undefined ? `✅` : `❌: ${Err}`
        }`
      );
    }
  }
  override async writeHashFields<T extends Record<string, unknown>>({
    context,
    expire,
    key,
    value,
    log,
  }: {
    context?: FUNCTIONS.Context;
    key: CACHE.KEY;
    value: Promise<T> | { [k in keyof T]: Promise<T[k]> | T[k] };
    expire: number;
    log?: boolean;
  }): Promise<void> {
    let awaitedValue: T;
    try {
      awaitedValue = (
        value instanceof Promise
          ? await value.catch(() => ({} as never))
          : value
      ) as T;
      for (const key in awaitedValue) {
        if (awaitedValue[key] instanceof Promise) {
          awaitedValue[key] = await awaitedValue[key].catch(() => undefined);
        }
        if (awaitedValue[key] === undefined) {
          delete awaitedValue[key];
        }
      }
      if (!Object.keys(awaitedValue).length) return;
    } catch {
      return;
    }
    const timer = time();
    let Err = undefined;
    await this.writeExe([
      key.toString(),
      Object.fromEntries(
        Object.keys(awaitedValue).map((key) => [
          key,
          this.opt.encode(awaitedValue[key]),
        ])
      ),
      expire,
    ]).catch((err) => {
      Err = err ?? null;
    });
    if (log) {
      (context ?? console).log(
        `(${timer()} ms) ${this.name}.write(${key}, [${
          //
          Object.keys(awaitedValue)
        }]) ${Err === undefined ? `✅` : `❌: ${Err}`}`
      );
    }
  }

  override async removeKey({
    context,
    key,
    log,
  }: {
    context?: FUNCTIONS.Context;
    key: CACHE.KEY;
    log?: boolean;
  }): Promise<void> {
    const timer = time();
    let Err = undefined;
    await this.removeExe([key.toString(), undefined]).catch((err) => {
      Err = err ?? null;
    });
    if (log) {
      (context ?? console).log(
        `(${timer()} ms) ${this.name}.remove(${key}) ${
          Err === undefined ? `✅` : `❌: ${Err}`
        }`
      );
    }
  }
  override async removeHashFields({
    context,
    fields,
    key,
    log,
  }: {
    context?: FUNCTIONS.Context;
    key: CACHE.KEY;
    fields: CACHE.AllFields | CACHE.KEY[];
    log?: boolean;
  }): Promise<void> {
    const timer = time();
    let Err = undefined;
    await this.removeExe([
      key.toString(),
      fields === "*" ? "*" : fields.map((x) => x.toString()),
    ]).catch((err) => {
      Err = err ?? null;
    });
    if (log) {
      (context ?? console).log(
        `(${timer()} ms) ${this.name}.remove(${key}, ${
          fields === "*" ? "*" : `[${fields}]`
        }) ${Err === undefined ? `✅` : `❌: ${Err}`}`
      );
    }
  }

  async increment({
    context,
    controller,
    params,
  }: {
    context?: FUNCTIONS.Context;
    controller: CACHE.CacheController | null;
    params: { key: string; incrBy: number; maxLimit?: number; expiry?: number };
  }): Promise<{ allowed: boolean; value: number }> {
    if (controller) {
      if (controller.client !== this)
        throw new Error("Invalid usage of Controller!");
      params.key = controller.getKey(params.key);
      if (!controller.can("increment")) return { allowed: false, value: 0 };
    }
    if (controller && params.expiry === undefined)
      params.expiry = controller.defaultExpiry;
    let error: unknown;
    const start = Date.now();
    const result = await this.client
      .eval(luaScripts.increment, {
        keys: [params.key],
        arguments: [
          `${params.incrBy}`,
          `${params.maxLimit || 0}`,
          `${params.expiry || 0}`,
        ],
      })
      .catch((err) => {
        error = err;
        return { allowed: false, value: 0 };
      });
    const timeTaken = Date.now() - start;
    if (!Array.isArray(result) || result.length !== 2)
      throw new Error("Unexpected response from Redis script!");
    log: {
      const isErr = error !== undefined;
      if (!isErr && !controller?.log) break log;
      let query = `(${timeTaken} ms) ++ ${this.name}.incr(${params.incrBy}, max: ${params.maxLimit})`;
      query += `\n\t.at(${params.key}): ${isErr ? "❌" : "✅"}`;
      (context ?? console).log(query, ...(isErr ? [error] : []));
    }
    return { allowed: !!result[0], value: +(result[1] as string | number) };
  }
}
