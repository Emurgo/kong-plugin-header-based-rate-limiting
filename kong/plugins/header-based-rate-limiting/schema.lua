return {
    no_consumer = true,
    -- local cache_key = kong.db.header_based_rate_limits:cache_key(service_id, route_id, rate_limit_subject:encoded_identifier())
    cache_key = { "service_id", "route_id", "rate_limit_subject" },
    fields = {
        redis = {
            type = "table",
            schema = {
                fields = {
                    host = { type = "string", required = true, default = "" },
                    port = { type = "number", required = true, default = 6379 },
                    db = { type = "number", required = true, default = 0 },
                    timeout_in_milliseconds = { type = "number", required = true, default = 1000 },
                    max_idle_timeout_in_milliseconds = { type = "number", required = true, default = 1000 },
                    pool_size = { type = "number", required = true, default = 10 }
                }
            }
        },
        default_rate_limit = { type = "number", required = true },
        log_only = { type = "boolean", required = true, default = false },
        identification_headers = { type = "array", required = true },
        forward_headers_to_upstream = { type = "boolean", default = false }
    }
}
