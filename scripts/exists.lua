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
