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
