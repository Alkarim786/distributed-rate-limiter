-- Sliding window log, atomic. Port of slidingLogStep in algorithms.go.
-- KEYS[1]  sorted set: score = admit time (ms), member = unique id
-- ARGV[1]  limit
-- ARGV[2]  window in ms
-- ARGV[3]  cost
-- ARGV[4]  nonce, unique per call (keeps members distinct within a ms)
-- Returns  {allowed, remaining, retry_ms, reset_ms}
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

-- An entry admitted at s counts while now < s + window.
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - window)
local count = redis.call('ZCARD', KEYS[1])

local allowed = 0
local retry = 0
if count + cost <= limit then
  for i = 1, cost do
    redis.call('ZADD', KEYS[1], now, t[1] .. t[2] .. '-' .. ARGV[4] .. '-' .. i)
  end
  count = count + cost
  allowed = 1
else
  -- The (count + cost - limit)-th oldest entry has to expire first.
  local idx = count + cost - limit - 1
  local e = redis.call('ZRANGE', KEYS[1], idx, idx, 'WITHSCORES')
  retry = tonumber(e[2]) + window - now
end

local reset = 0
local last = redis.call('ZRANGE', KEYS[1], -1, -1, 'WITHSCORES')
if last[2] then
  reset = tonumber(last[2]) + window - now
end
redis.call('PEXPIRE', KEYS[1], window + 1000)

local remaining = limit - count
if remaining < 0 then remaining = 0 end
return {allowed, remaining, retry, reset}
