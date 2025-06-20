/**
 * Redis Cache Client for C.CacheController
 *
 * @module
 *
 * @example
 * ```ts
 * import { C } from "@panth977/cache";
 * import { RedisCacheClient } from "@panth977/cache-redis";
 * import * as redis from "redis";
 *
 * const cache = new C.CacheController({
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

import type { RedisClientType, RedisDefaultModules, RedisFunctions, RedisModules, RedisScripts } from "redis";
import { C } from "@panth977/cache";
import { F } from "@panth977/functions";
import { T } from "@panth977/tools";
import * as fs from "fs";
import * as path from "path";
import * as url from "url";
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
const scriptsDir = (function () {
  const __filename = url.fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  return path.join(__dirname, "scripts");
})();
/**
 * Scripts used for queering redis
 */
const luaScripts = {
  exists: fs.readFileSync(path.join(scriptsDir, "exists.lua")).toString(),
  read: fs.readFileSync(path.join(scriptsDir, "read.lua")).toString(),
  write: fs.readFileSync(path.join(scriptsDir, "write.lua")).toString(),
  remove: fs.readFileSync(path.join(scriptsDir, "remove.lua")).toString(),
  increment: fs.readFileSync(path.join(scriptsDir, "increment.lua")).toString(),
};
type _RedisDefaultModules_ = RedisDefaultModules & Record<never, never>;
type _RedisFunctions_ = Record<string, never>;
type _RedisScripts_ = Record<string, never>;

function buildWithType<A, R>(
  func: <
    M extends RedisModules = _RedisDefaultModules_,
    F extends RedisFunctions = _RedisFunctions_,
    S extends RedisScripts = _RedisScripts_,
  >(client: RedisClientType<M, F, S>, cmds: A[]) => Promise<R[]>,
): <
  M extends RedisModules = _RedisDefaultModules_,
  F extends RedisFunctions = _RedisFunctions_,
  S extends RedisScripts = _RedisScripts_,
>(client: RedisClientType<M, F, S>, cmds: A[], cb: (r: ["Error", unknown] | ["Data", R[]]) => void) => void {
  return async function (client, cmds, cb) {
    try {
      const result = await func(client, cmds);
      cb(["Data", result]);
    } catch (err) {
      cb(["Error", err]);
    }
  };
}

type ExistsCmd = [string, string[] | "*" | undefined];
type ExistsRet = boolean | Record<string, boolean> | null;
const existsExe = buildWithType<ExistsCmd, ExistsRet>(async function (client, cmds) {
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
      for (let i = 0; i < item.length; i += 2) {
        obj[item[i]] = !!+item[i + 1];
      }
      values.push(obj);
    } else {
      values.push(null);
    }
  }
  return values;
});

type ReadCmd = [string, string[] | "*" | undefined];
type ReadRet = string | Record<string, string> | null;
const readExe = buildWithType<ReadCmd, ReadRet>(async function (client, cmds) {
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
});

type WriteCmd = [string, string | Record<string, string>, number];
type WriteRet = void;
const writeExe = buildWithType<WriteCmd, WriteRet>(async function (client, cmds) {
  await client.eval(luaScripts.write, {
    keys: cmds.map((x) => x[0]),
    arguments: cmds.map((x) => JSON.stringify([x[1], x[2]])),
  });
  return Array(cmds.length);
});

type RemoveCmd = [string, string[] | "*" | undefined];
type RemoveRet = void;
const removeExe = buildWithType<RemoveCmd, RemoveRet>(async function (client, cmds) {
  await client.eval(luaScripts.remove, {
    keys: cmds.map((p) => p[0]),
    arguments: cmds.map((p) => JSON.stringify(p[1] || null)),
  });
  return Array(cmds.length);
});

type IncrementCmd = [string, string | null, number, number | null, number];
type IncrementRet = [boolean, number];
const incrementExe = buildWithType<IncrementCmd, IncrementRet>(async function (client, cmds) {
  const result = await client.eval(luaScripts.increment, {
    keys: cmds.map((p) => p[0]),
    arguments: cmds.map((p) => JSON.stringify([p[1], p[2], p[3], p[4]])),
  });
  return (result as [number, number][]).map((x) => [!!x[0], x[1]]);
});

/**
 * Use this as a client for C.CacheController
 */ export class RedisCacheClient<
  M extends RedisModules = _RedisDefaultModules_,
  F extends RedisFunctions = _RedisFunctions_,
  S extends RedisScripts = _RedisScripts_,
> extends C.CacheController {
  constructor(
    opt: {
      name: string;
      separator: string;
      expiry: number;
      prefix: string;
      log: boolean;
      mode: "read-write" | "readonly" | "writeonly";
    },
    protected redis: {
      client: RedisClientType<M, F, S>;
      decode: <T>(val: string) => T;
      encode: <T>(val: T) => string;
      delayInMs: number;
    },
    protected exe: {
      exists: T.CreateBatch<ExistsCmd, ExistsRet>;
      read: T.CreateBatch<ReadCmd, ReadRet>;
      write: T.CreateBatch<WriteCmd, WriteRet>;
      remove: T.CreateBatch<RemoveCmd, RemoveRet>;
      increment: T.CreateBatch<IncrementCmd, IncrementRet>;
    } = {
      exists: new T.CreateBatch(existsExe.bind(null, redis.client), redis.delayInMs),
      read: new T.CreateBatch(readExe.bind(null, redis.client), redis.delayInMs),
      write: new T.CreateBatch(writeExe.bind(null, redis.client), redis.delayInMs),
      remove: new T.CreateBatch(removeExe.bind(null, redis.client), redis.delayInMs),
      increment: new T.CreateBatch(incrementExe.bind(null, redis.client), redis.delayInMs),
    },
  ) {
    super(opt);
  }
  get client(): RedisClientType<M, F, S> {
    return this.redis.client;
  }
  private logger(context: F.Context, prefix: string, start: number) {
    context.logMsg(prefix, `${Date.now() - start} ms`);
  }
  private portToCb<T>(logger: null | (() => void), port: F.AsyncCbSender<T>, r: ["Error", unknown] | ["Data", T]): void {
    logger?.();
    if (r[0] === "Data") {
      port.return(r[1]);
    } else {
      port.throw(r[1]);
    }
  }
  protected toReciver<A, R>({ context, exe, logInfo: [logMethod, ...logArgs], arg }: {
    exe: T.CreateBatch<A, R>;
    context: F.Context;
    logInfo: ["exists" | "read" | "write" | "remove" | "increment", ...any[]];
    arg: A;
  }): F.AsyncCbReceiver<R> {
    const timer = this.log ? this.logger.bind(this, context, `${this.name}.${logMethod}(${logArgs})`, Date.now()) : null;
    const port = new F.AsyncCbSender<R>();
    try {
      exe.runJob("cb", arg, (this.portToCb<R>).bind(this, timer, port));
    } catch (err) {
      port.throw(err);
    }
    return port.getHandler();
  }
  private _exitstsRetToBool(value: ExistsRet): boolean {
    if (typeof value === "boolean") return value;
    return false;
  }
  override existsKeyCb(
    context: F.Context,
    opt: { key?: C.KEY },
  ): F.AsyncCbReceiver<boolean> {
    if (this.canExeExists()) {
      return F.AsyncCbReceiver.error(new Error("Method not allowed"));
    }
    const key = this._getKey(opt.key);
    return this.toReciver<ExistsCmd, ExistsRet>({
      context,
      exe: this.exe.exists,
      arg: [key, undefined],
      logInfo: ["exists", key],
    }).pipeThen(this._exitstsRetToBool.bind(this));
  }
  private _exitstsRetToHashBool(value: ExistsRet): Record<string, boolean> {
    if (typeof value === "boolean") return {};
    return value ?? {};
  }
  override existsHashFieldsCb(
    context: F.Context,
    opt: { key?: C.KEY; fields: C.KEY[] | C.AllFields },
  ): F.AsyncCbReceiver<Record<string, boolean>> {
    if (this.canExeExists()) {
      return F.AsyncCbReceiver.error(new Error("Method not allowed"));
    }
    const key = this._getKey(opt.key);
    return this.toReciver<ExistsCmd, ExistsRet>({
      context,
      exe: this.exe.exists,
      arg: [key, opt.fields === "*" ? "*" : opt.fields.map((x) => x.toString())],
      logInfo: ["exists", key],
    }).pipeThen(this._exitstsRetToHashBool.bind(this));
  }
  private _readRetToVal<T>(value: ReadRet): T | undefined {
    if (value === undefined) return undefined;
    if (typeof value === "string") {
      return this.redis.decode(value);
    }
    throw new Error("Unknown Type");
  }
  override readKeyCb<T>(
    context: F.Context,
    opt: { key?: C.KEY },
  ): F.AsyncCbReceiver<T | undefined> {
    if (this.canExeRead()) {
      return F.AsyncCbReceiver.error(new Error("Method not allowed"));
    }
    const key = this._getKey(opt.key);
    return this.toReciver<ReadCmd, ReadRet>({
      context,
      exe: this.exe.read,
      arg: [key, undefined],
      logInfo: ["read", key],
    }).pipeThen((this._readRetToVal<T>).bind(this));
  }
  private _readRetToHashVal<T extends Record<string, unknown>>(value: ReadRet): Partial<T> {
    if (value === undefined) return {};
    if (typeof value === "string") {
      throw new Error("Unknown Type");
    }
    const ret: Partial<T> = {};
    for (const key in value) {
      if (typeof value[key] === "string") {
        (ret as any)[key] = this.redis.decode(value[key]);
      }
    }
    return ret;
  }
  override readHashFieldsCb<T extends Record<string, unknown>>(
    context: F.Context,
    opt: { key?: C.KEY; fields: C.KEY[] | C.AllFields },
  ): F.AsyncCbReceiver<Partial<T>> {
    if (this.canExeRead()) {
      return F.AsyncCbReceiver.error(new Error("Method not allowed"));
    }
    const key = this._getKey(opt.key);
    return this.toReciver<ReadCmd, ReadRet>({
      context,
      exe: this.exe.read,
      arg: [key, opt.fields === "*" ? "*" : opt.fields.map((x) => x.toString())],
      logInfo: ["read", key],
    }).pipeThen((this._readRetToHashVal<T>).bind(this));
  }
  override writeKeyCb<T>(
    context: F.Context,
    opt: { key?: C.KEY; value: T },
  ): F.AsyncCbReceiver<void> {
    if (this.canExeWrite()) {
      return F.AsyncCbReceiver.error(new Error("Method not allowed"));
    }
    const key = this._getKey(opt.key);
    const value = this.redis.encode(opt.value);
    return this.toReciver<WriteCmd, WriteRet>({
      context,
      exe: this.exe.write,
      arg: [key, value, this.expiry],
      logInfo: ["write", key],
    });
  }
  override writeHashFieldsCb<T extends Record<string, unknown>>(
    context: F.Context,
    opt: { key?: C.KEY; value: T },
  ): F.AsyncCbReceiver<void> {
    if (this.canExeWrite()) {
      return F.AsyncCbReceiver.error(new Error("Method not allowed"));
    }
    const key = this._getKey(opt.key);
    const value = Object.fromEntries(
      Object.keys(opt.value).map((key) => [
        key,
        this.redis.encode(opt.value[key]),
      ]),
    );
    return this.toReciver<WriteCmd, WriteRet>({
      context,
      exe: this.exe.write,
      arg: [key, value, this.expiry],
      logInfo: ["write", key],
    });
  }

  override removeKeyCb(
    context: F.Context,
    opt: { key?: C.KEY },
  ): F.AsyncCbReceiver<void> {
    if (this.canExeRemove()) {
      return F.AsyncCbReceiver.error(new Error("Method not allowed"));
    }
    const key = this._getKey(opt.key);
    return this.toReciver<RemoveCmd, RemoveRet>({
      context,
      exe: this.exe.remove,
      arg: [key, undefined],
      logInfo: ["exists", key],
    });
  }
  override removeHashFieldsCb(
    context: F.Context,
    opt: { key?: C.KEY; fields: C.KEY[] | C.AllFields },
  ): F.AsyncCbReceiver<void> {
    if (this.canExeExists()) {
      return F.AsyncCbReceiver.error(new Error("Method not allowed"));
    }
    const key = this._getKey(opt.key);
    return this.toReciver<RemoveCmd, RemoveRet>({
      context,
      exe: this.exe.remove,
      arg: [key, opt.fields === "*" ? "*" : opt.fields.map((x) => x.toString())],
      logInfo: ["exists", key],
    });
  }
  private _incrementRetToVal(value: IncrementRet): { allowed: boolean; value: number } {
    return { allowed: value[0], value: value[1] };
  }
  override incrementKeyCb(
    context: F.Context,
    opt: { key?: C.KEY; incrBy: number; maxLimit: number },
  ): F.AsyncCbReceiver<{ allowed: boolean; value: number }> {
    if (this.canExeIncrement()) {
      return F.AsyncCbReceiver.error(new Error("Method not allowed"));
    }
    const key = this._getKey(opt.key);
    return this.toReciver<IncrementCmd, IncrementRet>({
      context,
      exe: this.exe.increment,
      arg: [key, null, opt.incrBy, opt.maxLimit ?? null, this.expiry],
      logInfo: ["exists", key],
    }).pipeThen(this._incrementRetToVal.bind(this));
  }
  override incrementHashFieldCb(
    context: F.Context,
    opt: { key?: C.KEY; field: C.KEY; incrBy: number; maxLimit: number },
  ): F.AsyncCbReceiver<{ allowed: boolean; value: number }> {
    if (this.canExeIncrement()) {
      return F.AsyncCbReceiver.error(new Error("Method not allowed"));
    }
    const key = this._getKey(opt.key);
    return this.toReciver<IncrementCmd, IncrementRet>({
      context,
      exe: this.exe.increment,
      arg: [key, opt.field.toString(), opt.incrBy, opt.maxLimit ?? null, this.expiry],
      logInfo: ["exists", key],
    }).pipeThen(this._incrementRetToVal.bind(this));
  }
  override dispose(): void {
    this.redis.client.close();
  }
  protected override clone(): this {
    return new RedisCacheClient(
      { expiry: this.expiry, log: this.log, mode: this.mode, name: this.name, prefix: this.prefix, separator: this.separator },
      this.redis,
      this.exe,
    ) as this;
  }
}
