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

import type {
  RedisClientType,
  RedisDefaultModules,
  RedisFunctions,
  RedisModules,
  RedisScripts,
} from "redis";
import { C } from "@panth977/cache";
import { T } from "@panth977/tools";
import * as fs from "fs";
import * as path from "path";
import * as url from "url";
import type { F } from "@panth977/functions";
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
  >(
    client: RedisClientType<M, F, S>,
    cmds: A[],
  ) => Promise<R[]>,
): <
  M extends RedisModules = _RedisDefaultModules_,
  F extends RedisFunctions = _RedisFunctions_,
  S extends RedisScripts = _RedisScripts_,
>(
  client: RedisClientType<M, F, S>,
  cmds: A[],
) => T.PPromise<R[]> {
  return function (client, cmds) {
    try {
      const result = func(client, cmds);
      return T.PPromise.from(result);
    } catch (err) {
      return T.PPromise.reject(err);
    }
  };
}

type ExistsCmd = [string, string[] | "*" | undefined];
type ExistsRet = boolean | Record<string, boolean> | null;
const existsExe = buildWithType<ExistsCmd, ExistsRet>(
  async function (client, cmds) {
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
  },
);

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
const writeExe = buildWithType<WriteCmd, WriteRet>(
  async function (client, cmds) {
    await client.eval(luaScripts.write, {
      keys: cmds.map((x) => x[0]),
      arguments: cmds.map((x) => JSON.stringify([x[1], x[2]])),
    });
    return Array(cmds.length);
  },
);

type RemoveCmd = [string, string[] | "*" | undefined];
type RemoveRet = void;
const removeExe = buildWithType<RemoveCmd, RemoveRet>(
  async function (client, cmds) {
    await client.eval(luaScripts.remove, {
      keys: cmds.map((p) => p[0]),
      arguments: cmds.map((p) => JSON.stringify(p[1] || null)),
    });
    return Array(cmds.length);
  },
);

type IncrementCmd = [string, string | null, number, number | null, number];
type IncrementRet = [boolean, number];
const incrementExe = buildWithType<IncrementCmd, IncrementRet>(
  async function (client, cmds) {
    const result = await client.eval(luaScripts.increment, {
      keys: cmds.map((p) => p[0]),
      arguments: cmds.map((p) => JSON.stringify([p[1], p[2], p[3], p[4]])),
    });
    return (result as [number, number][]).map((x) => [!!x[0], x[1]]);
  },
);

/**
 * Use this as a client for C.CacheController
 */ export class RedisCacheClient<
  RM extends RedisModules = _RedisDefaultModules_,
  RF extends RedisFunctions = _RedisFunctions_,
  RS extends RedisScripts = _RedisScripts_,
> extends C.CacheController {
  constructor(
    opt: {
      name: string;
      separator: string;
      expiry: number;
      prefix: string;
      log: boolean;
      mode: "read-write" | "readonly" | "writeonly" | "ignore";
    },
    protected redis: {
      client: RedisClientType<RM, RF, RS>;
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
      exists: new T.CreateBatch(
        existsExe.bind(null, redis.client),
        redis.delayInMs,
      ),
      read: new T.CreateBatch(
        readExe.bind(null, redis.client),
        redis.delayInMs,
      ),
      write: new T.CreateBatch(
        writeExe.bind(null, redis.client),
        redis.delayInMs,
      ),
      remove: new T.CreateBatch(
        removeExe.bind(null, redis.client),
        redis.delayInMs,
      ),
      increment: new T.CreateBatch(
        incrementExe.bind(null, redis.client),
        redis.delayInMs,
      ),
    },
  ) {
    super(opt);
  }
  get client(): RedisClientType<RM, RF, RS> {
    return this.redis.client;
  }
  private logger(context: F.Context, prefix: string, start: number) {
    context.logMsg(prefix, `${Date.now() - start} ms`);
  }
  protected toReciver<A, R>({
    context,
    exe,
    logInfo: [logMethod, ...logArgs],
    arg,
  }: {
    exe: T.CreateBatch<A, R>;
    context: F.Context;
    logInfo: ["exists" | "read" | "write" | "remove" | "increment", ...any[]];
    arg: A;
  }): T.PPromise<R> {
    const timer = this.log
      ? this.logger.bind(
          this,
          context,
          `${this.name}.${logMethod}(${logArgs})`,
          Date.now(),
        )
      : null;
    const [port, promise] = T.$async<R>(false);
    try {
      const process = exe.runJob(arg);
      if (timer) process.onend(timer);
      process.ondata(port.return);
      process.onerror(port.throw);
    } catch (err) {
      port.throw(err);
    }
    return promise;
  }
  private _exitstsRetToBool(value: ExistsRet): boolean {
    if (typeof value === "boolean") return value;
    return false;
  }
  override existsKey(
    context: F.Context,
    opt: { key?: C.KEY },
  ): T.PPromise<boolean> {
    if (this.canExeExists()) {
      return T.PPromise.reject(new Error("Method not allowed"));
    }
    const key = this._getKey(opt.key);
    return this.toReciver<ExistsCmd, ExistsRet>({
      context,
      exe: this.exe.exists,
      arg: [key, undefined],
      logInfo: ["exists", key],
    }).map(this._exitstsRetToBool.bind(this));
  }
  private _exitstsRetToHashBool(value: ExistsRet): Record<string, boolean> {
    if (typeof value === "boolean") return {};
    return value ?? {};
  }
  override existsHashFields(
    context: F.Context,
    opt: { key?: C.KEY; fields: C.KEY[] | C.AllFields },
  ): T.PPromise<Record<string, boolean>> {
    if (this.canExeExists()) {
      return T.PPromise.reject(new Error("Method not allowed"));
    }
    const key = this._getKey(opt.key);
    return this.toReciver<ExistsCmd, ExistsRet>({
      context,
      exe: this.exe.exists,
      arg: [
        key,
        opt.fields === "*" ? "*" : opt.fields.map((x) => x.toString()),
      ],
      logInfo: ["exists", key],
    }).map(this._exitstsRetToHashBool.bind(this));
  }
  private _readRetToVal<T>(value: ReadRet): T | undefined {
    if (value === undefined) return undefined;
    if (typeof value === "string") {
      return this.redis.decode(value);
    }
    throw new Error("Unknown Type");
  }
  override readKey<T>(
    context: F.Context,
    opt: { key?: C.KEY },
  ): T.PPromise<T | undefined> {
    if (this.canExeRead()) {
      return T.PPromise.reject(new Error("Method not allowed"));
    }
    const key = this._getKey(opt.key);
    return this.toReciver<ReadCmd, ReadRet>({
      context,
      exe: this.exe.read,
      arg: [key, undefined],
      logInfo: ["read", key],
    }).map((this._readRetToVal<T>).bind(this));
  }
  private _readRetToHashVal<T extends Record<string, unknown>>(
    value: ReadRet,
  ): Partial<T> {
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
  override readHashFields<T extends Record<string, unknown>>(
    context: F.Context,
    opt: { key?: C.KEY; fields: C.KEY[] | C.AllFields },
  ): T.PPromise<Partial<T>> {
    if (this.canExeRead()) {
      return T.PPromise.reject(new Error("Method not allowed"));
    }
    const key = this._getKey(opt.key);
    return this.toReciver<ReadCmd, ReadRet>({
      context,
      exe: this.exe.read,
      arg: [
        key,
        opt.fields === "*" ? "*" : opt.fields.map((x) => x.toString()),
      ],
      logInfo: ["read", key],
    }).map((this._readRetToHashVal<T>).bind(this));
  }
  override writeKey<T>(
    context: F.Context,
    opt: { key?: C.KEY; value: T },
  ): T.PPromise<void> {
    if (this.canExeWrite()) {
      return T.PPromise.reject(new Error("Method not allowed"));
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
  override writeHashFields<T extends Record<string, unknown>>(
    context: F.Context,
    opt: { key?: C.KEY; value: T },
  ): T.PPromise<void> {
    if (this.canExeWrite()) {
      return T.PPromise.reject(new Error("Method not allowed"));
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

  override removeKey(
    context: F.Context,
    opt: { key?: C.KEY },
  ): T.PPromise<void> {
    if (this.canExeRemove()) {
      return T.PPromise.reject(new Error("Method not allowed"));
    }
    const key = this._getKey(opt.key);
    return this.toReciver<RemoveCmd, RemoveRet>({
      context,
      exe: this.exe.remove,
      arg: [key, undefined],
      logInfo: ["exists", key],
    });
  }
  override removeHashFields(
    context: F.Context,
    opt: { key?: C.KEY; fields: C.KEY[] | C.AllFields },
  ): T.PPromise<void> {
    if (this.canExeExists()) {
      return T.PPromise.reject(new Error("Method not allowed"));
    }
    const key = this._getKey(opt.key);
    return this.toReciver<RemoveCmd, RemoveRet>({
      context,
      exe: this.exe.remove,
      arg: [
        key,
        opt.fields === "*" ? "*" : opt.fields.map((x) => x.toString()),
      ],
      logInfo: ["exists", key],
    });
  }
  private _incrementRetToVal(value: IncrementRet): {
    allowed: boolean;
    value: number;
  } {
    return { allowed: value[0], value: value[1] };
  }
  override incrementKey(
    context: F.Context,
    opt: { key?: C.KEY; incrBy: number; maxLimit: number },
  ): T.PPromise<{ allowed: boolean; value: number }> {
    if (this.canExeIncrement()) {
      return T.PPromise.reject(new Error("Method not allowed"));
    }
    const key = this._getKey(opt.key);
    return this.toReciver<IncrementCmd, IncrementRet>({
      context,
      exe: this.exe.increment,
      arg: [key, null, opt.incrBy, opt.maxLimit ?? null, this.expiry],
      logInfo: ["exists", key],
    }).map(this._incrementRetToVal.bind(this));
  }
  override incrementHashField(
    context: F.Context,
    opt: { key?: C.KEY; field: C.KEY; incrBy: number; maxLimit: number },
  ): T.PPromise<{ allowed: boolean; value: number }> {
    if (this.canExeIncrement()) {
      return T.PPromise.reject(new Error("Method not allowed"));
    }
    const key = this._getKey(opt.key);
    return this.toReciver<IncrementCmd, IncrementRet>({
      context,
      exe: this.exe.increment,
      arg: [
        key,
        opt.field.toString(),
        opt.incrBy,
        opt.maxLimit ?? null,
        this.expiry,
      ],
      logInfo: ["exists", key],
    }).map(this._incrementRetToVal.bind(this));
  }
  override dispose(): void {
    this.redis.client.close();
  }
  protected override clone(): this {
    return new RedisCacheClient(
      {
        expiry: this.expiry,
        log: this.log,
        mode: this.mode,
        name: this.name,
        prefix: this.prefix,
        separator: this.separator,
      },
      this.redis,
      this.exe,
    ) as this;
  }
}
