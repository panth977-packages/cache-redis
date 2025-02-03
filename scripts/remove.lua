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

