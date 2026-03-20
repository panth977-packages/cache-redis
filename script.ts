export const luaScripts = {
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

    if type(value) == 'string' then
        if keyType ~= 'string' and keyType ~= 'none' then
            redis.call('del', key)
        end
        redis.call('set', key, value)
        if exp > 0 then
            redis.call('expire', key, exp)
        end

    elseif type(value) == 'table' then
        if keyType ~= 'hash' and keyType ~= 'none' then
            redis.call('del', key)
        end
        for field, fieldValue in pairs(value) do
            redis.call('hset', key, field, fieldValue)
        end
        if keyType ~= 'hash' and exp > 0 then
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
  `,
  increment: `
local null = cjson.decode('null')
local results = {}
for i, key in ipairs(KEYS) do
    local args = cjson.decode(ARGV[i])
    local field = args[1]
    local incrBy = tonumber(args[2])
    local maxLimit = tonumber(args[3])
    local expirySec = tonumber(args[4])
    local keyType = redis.call('type', key).ok
    if field == null then
        if keyType ~= 'string' and keyType ~= 'none' then
            redis.call('del', key)
        end
        local currentValue = tonumber(redis.call('get', key) or '0')
        local allowed = 1
        if maxLimit and maxLimit > 0 and currentValue + incrBy > maxLimit then
            allowed = 0
        end
        if allowed == 1 then
            currentValue = redis.call('incrby', key, incrBy)
            if expirySec and expirySec > 0 then
                redis.call('expire', key, expirySec)
            end
        end
        table.insert(results, {allowed, currentValue})
    else
        if keyType ~= 'hash' and keyType ~= 'none' then
            redis.call('del', key)
        end
        local currentValue = tonumber(redis.call('hget', key, field) or '0')
        local allowed = 1
        if maxLimit and maxLimit > 0 and currentValue + incrBy > maxLimit then
            allowed = 0
        end
        if allowed == 1 then
            currentValue = redis.call('hincrby', key, field, incrBy)
            if expirySec and expirySec > 0 then
                redis.call('expire', key, expirySec)
            end
        end
        table.insert(results, {allowed, currentValue})
    end
end
return results
  `,
};
