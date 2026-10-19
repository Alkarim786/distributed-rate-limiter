-- Sliding window counter, atomic. Port of slidingCounterStep in algorithms.go.
-- KEYS[1]  hash {w = current window index, c = current count, p = previous count}
-- ARGV[1]  limit
-- ARGV[2]  window in ms
-- ARGV[3]  cost
-- Returns  {allowed, remaining, retry_ms, reset_ms}
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local idx = math.floor(now / window)

local st = redis.call('HMGET', KEYS[1], 'w', 'c', 'p')
local w = tonumber(st[1])
local curr = tonumber(st[2]) or 0
local prev = tonumber(st[3]) or 0
if w == nil then
  w, curr, prev = idx, 0, 0
elseif idx == w + 1 then
  prev, curr, w = curr, 0, idx
elseif idx > w + 1 then
  prev, curr, w = 0, 0, idx
end

local elapsed = now - idx * window
local est = prev * ((window - elapsed) / window) + curr

local allowed = 0
local retry = 0
if est + cost <= limit then
  curr = curr + cost
  est = est + cost
  allowed = 1
elseif curr + cost <= limit and prev > 0 then
  local x = window * (1 - (limit - curr - cost) / prev)
  retry = math.max(1, math.ceil(x - 1e-9) - elapsed)
else
  local x = window * (1 - (limit - cost) / curr)
  retry = math.max(1, (window - elapsed) + math.ceil(math.max(0, x) - 1e-9))
end

local remaining = math.floor(limit - est)
if remaining < 0 then remaining = 0 end
local reset = 0
if curr > 0 then
  reset = (window - elapsed) + window
elseif prev > 0 then
  reset = window - elapsed
end

redis.call('HSET', KEYS[1], 'w', w, 'c', curr, 'p', prev)
redis.call('PEXPIRE', KEYS[1], 2 * window + 1000)
return {allowed, remaining, retry, reset}
