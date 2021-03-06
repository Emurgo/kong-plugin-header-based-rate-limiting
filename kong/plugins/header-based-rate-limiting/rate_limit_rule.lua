local Object = require "classic"
local LookupKeyGenerator = require "kong.plugins.header-based-rate-limiting.lookup_key_generator"
local KeyRank = require "kong.plugins.header-based-rate-limiting.key_rank"
local inspect = require "inspect"

local function select_most_specific_rule(rules)
    local most_specific = rules[1]

    for i = 2, #rules do
        local rule = rules[i]
        if KeyRank(most_specific.header_composition) < KeyRank(rule.header_composition) then
            most_specific = rule
        end
    end

    return most_specific
end

local function split_header_composition(s, pattern, maxsplit)
  local pattern = pattern or ','
  local maxsplit = maxsplit or -1
  local s = s
  local t = {}
  local patsz = #pattern
  while maxsplit ~= 0 do
    local curpos = 1
    local found = string.find(s, pattern)
    if found ~= nil then
      table.insert(t, string.sub(s, curpos, found - 1))
      curpos = found + patsz
      s = string.sub(s, curpos)
    else
      table.insert(t, string.sub(s, curpos))
      break
    end
    maxsplit = maxsplit - 1
    if maxsplit == 0 then
      table.insert(t, string.sub(s, curpos - patsz - 1))
    end
  end
  return t
end

local function find_applicable_rate_limit(model, service_id, route_id, entity_identifier)
    local compositions_with_fallback = LookupKeyGenerator.from_list(entity_identifier)
    local custom_rate_limits = model:get(service_id, route_id, compositions_with_fallback)
    local most_specific_rate_limit = select_most_specific_rule(custom_rate_limits)

    -- if specific limits by static encoded headers were not found, try with regexp limits
    if most_specific_rate_limit == nil then

      -- weigth matches by number of regexp matches in each rule
      local weighted_matchs = {}
      local regexp_limits = model:get_regexp(service_id, route_id)
      for k, limit in pairs(regexp_limits) do

        weighted_matchs[limit.id] = 0
        for index, header in ipairs(split_header_composition(limit.header_composition)) do
          local decoded_regexp = ngx.decode_base64(header:gsub("^regexp_",""))
          if string.len(decoded_regexp) >= 1 then
            if string.find(ngx.decode_base64(entity_identifier[index]), decoded_regexp) ~= nil then
               weighted_matchs[limit.id] = weighted_matchs[limit.id] + 1
            end
          end
        end

      end
      -- find the rule id that matched the most
      local value, highest_match_rule_id = -math.huge
      for k, v in pairs(weighted_matchs) do
          if v > value then
              value, highest_match_rule_id = v, k
          end
      end

      -- actually get the limit from the set and return it
      if weighted_matchs[highest_match_rule_id] >= 1 then
        for key, most_specific_rate_limit in pairs(regexp_limits) do
          if most_specific_rate_limit.id == highest_match_rule_id then
            return most_specific_rate_limit and most_specific_rate_limit.rate_limit
          end
        end
      end
    end

    return most_specific_rate_limit and most_specific_rate_limit.rate_limit
end

local RateLimitRule = Object:extend()

function RateLimitRule:new(model, default_rate_limit)
    self.model = model
    self.default_rate_limit = default_rate_limit
end

function RateLimitRule:find(service_id, route_id, subject)
    local entity_identifier = subject:encoded_identifier_array()
    local rate_limit_from_rules = find_applicable_rate_limit(self.model, service_id, route_id, entity_identifier)

    return rate_limit_from_rules or self.default_rate_limit
end

return RateLimitRule
