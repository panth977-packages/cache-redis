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
import * as fs from 'fs';
import * as path from 'path';
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
const luaScripts = {
  exists: fs.readFileSync(path.join(__dirname, 'scripts', 'exists.lua')).toString(),
  read: fs.readFileSync(path.join(__dirname, 'scripts', 'read.lua')).toString(),
  write: fs.readFileSync(path.join(__dirname, 'scripts', 'write.lua')).toString(),
  remove: fs.readFileSync(path.join(__dirname, 'scripts', 'remove.lua')).toString(),
  increment: fs.readFileSync(path.join(__dirname, 'scripts', 'increment.lua')).toString(),
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
  readonly incrementExe: (
    cmd: [string, string | null, number, number | null, number]
  ) => Promise<[boolean, number]>;
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
          arguments: cmds.map((x) => JSON.stringify([x[1], x[2]])),
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
    this.incrementExe = TOOLS.CreateBatchProcessor({
      delayInMs: timeout,
      async implementation(cmds) {
        const result = await client.eval(luaScripts.increment, {
          keys: cmds.map((p) => p[0]),
          arguments: cmds.map((p) => JSON.stringify([p[1], p[2], p[3], p[4]])),
        });
        return (result as [number, number][]).map((x) => [!!x[0], x[1]]);
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
    expiry,
    key,
    value,
    log,
  }: {
    context?: FUNCTIONS.Context;
    key: CACHE.KEY;
    value: T | Promise<T>;
    expiry: number;
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
    await this.writeExe([key.toString(), val, expiry]).catch((err) => {
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
    expiry,
    key,
    value,
    log,
  }: {
    context?: FUNCTIONS.Context;
    key: CACHE.KEY;
    value: Promise<T> | { [k in keyof T]: Promise<T[k]> | T[k] };
    expiry: number;
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
      expiry,
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
  override async incrementKey({
    expiry,
    incrBy,
    key,
    context,
    log,
    maxLimit,
  }: {
    context?: FUNCTIONS.Context;
    key: CACHE.KEY;
    incrBy: number;
    maxLimit?: number;
    expiry: number;
    log?: boolean;
  }): Promise<{ allowed: boolean; value: number }> {
    const timer = time();
    let Err = undefined;
    const [allowed, value] = await this.incrementExe([
      key.toString(),
      null,
      incrBy,
      maxLimit ?? null,
      expiry,
    ]).catch((err) => {
      Err = err ?? null;
      return [false, 0] as const;
    });
    if (log) {
      (context ?? console).log(
        `(${timer()} ms) ${this.name}.increment(${key}) ${
          Err === undefined ? `✅` : `❌: ${Err}`
        }`
      );
    }
    return { allowed, value };
  }
  override async incrementHashField({
    expiry,
    incrBy,
    key,
    field,
    context,
    log,
    maxLimit,
  }: {
    context?: FUNCTIONS.Context;
    key: CACHE.KEY;
    field: CACHE.KEY;
    incrBy: number;
    maxLimit?: number;
    expiry: number;
    log?: boolean;
  }): Promise<{ allowed: boolean; value: number }> {
    const timer = time();
    let Err = undefined;
    const [allowed, value] = await this.incrementExe([
      key.toString(),
      field.toString(),
      incrBy,
      maxLimit ?? null,
      expiry,
    ]).catch((err) => {
      Err = err ?? null;
      return [false, 0] as const;
    });
    if (log) {
      (context ?? console).log(
        `(${timer()} ms) ${this.name}.increment(${key}, ${field}) ${
          Err === undefined ? `✅` : `❌: ${Err}`
        }`
      );
    }
    return { allowed, value };
  }
}
