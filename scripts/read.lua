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
