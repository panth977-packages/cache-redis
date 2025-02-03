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
        if exp > 0 then
            redis.call('expire', key, exp)
        end
    end
end
