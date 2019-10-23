local Redis = require "resty.redis"

local function set_timeout(redis, timeout_in_ms)
    local success, err = pcall(redis.set_timeout, redis, timeout_in_ms)

    if not success then
        error({
            msg = "Error while setting Redis timeout",
            reason = err
        })
    end
end

local function connect(redis, host, port)
    local success, err = redis:connect(host, port)

    if not success then
        error({
            msg = "Could not connect to Redis",
            reason = err
        })
    end
end

local function select_db(redis, db)
    local success, err = redis:select(db)

    if not success then
        error({
            msg = "Could not select Redis DB",
            reason = err
        })
    end
end

return {
    create = function(host, port, db, timeout_in_milliseconds)
        local redis = Redis:new()

        set_timeout(redis, timeout_in_milliseconds or 1000)

        connect(redis, host, port)

        select_db(redis, db)

        return redis
    end
}
