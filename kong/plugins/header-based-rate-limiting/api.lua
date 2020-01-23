local cjson = require "cjson"
local split = require("kong.tools.utils").split
local RedisFactory = require "kong.plugins.header-based-rate-limiting.redis_factory"
local dao_plugins  = kong.db.plugins
local hbrl_schema = kong.db.header_based_rate_limits.schema
local endpoints = require "kong.api.endpoints"
local inspect	    = require "inspect"

local decode_base64 = ngx.decode_base64
local encode_base64 = ngx.encode_base64

local function decode_headers(encoded_header_composition)
    local individual_headers = split(encoded_header_composition, ",")
    local decoded_headers = {}

    for _, header in ipairs(individual_headers) do
        local decoded_header = header == "*" and "*" or decode_base64(header)

        table.insert(decoded_headers, decoded_header)
    end

    return decoded_headers
end

local function decode_header_composition(header_based_rate_limit)
    local result = {}

    for key, value in pairs(header_based_rate_limit) do
        if key == "header_composition" then
            result["header_composition"] = decode_headers(value)
        else
            result[key] = value
        end
    end

    return result
end

local function is_wildcard(header)
    return header == "*" or header == cjson.null
end

local function contains_regexp(header)
    if string.find(header, ".*", 0, true) then
        return true
    else
        return false
    end
end

local function encode_headers(header_composition)
    local encoded_headers = {}

    for _, header in ipairs(header_composition) do
        local encoded_header = is_wildcard(header)
        if contains_regexp(header) == true then
            encoded_header = "regexp_" .. encode_base64(header)
        else
            encoded_header = encode_base64(header)
        end

        table.insert(encoded_headers, encoded_header)
    end

    return table.concat(encoded_headers, ",")
end

local function trim_postfix_wildcards(encoded_header_composition)
    return select(1, encoded_header_composition:gsub("[,*]+$", ""))
end

local function encode_header_composition(header_based_rate_limit)
    local result = {}

    for key, value in pairs(header_based_rate_limit) do
        if key == "header_composition" then
            result["header_composition"] = trim_postfix_wildcards(encode_headers(value))
        else
            result[key] = value
        end
    end

    return result
end

return {
    ["/header-based-rate-limits"] = {
        schema = hbrl_schema,
        methods = {
            POST = function(self, ...)
                local params_with_encoded_header_composition = encode_header_composition(self.args.post)
                self.args.post = params_with_encoded_header_composition
                endpoints.post_collection_endpoint(hbrl_schema)(self, ...)
            end,

            GET = function(self, ...)
                endpoints.get_collection_endpoint(hbrl_schema)(self, ...)
            end,

            DELETE = function(self, ...)
                kong.db.header_based_rate_limits:truncate()
                return kong.response.exit(200, { message = "Dropped all the header-based rate limits" })
            end
        }
    },

    ["/header-based-rate-limits/:id"] = {
        schema = hbrl_schema,
        methods = {
            DELETE = function(self, ...)
	              local res, err = kong.db.connector:query(string.format(
                    "DELETE FROM header_based_rate_limits WHERE id = '%s'", self.params.id))

                    if err or res.affected_rows == 0 then
	                      return kong.response.exit(404, { message = "ratelimit not found!" })
                    end
	              return kong.response.exit(200, { message = "ratelimit deleted!" })

            end
        }
    }
}
