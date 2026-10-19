-- Token bucket, atomic. Port of tokenBucketStep in algorithms.go.
-- KEYS[1]  bucket hash {tokens, ts}
-- ARGV[1]  capacity (= rule limit)
-- ARGV[2]  window in ms (time to refill a full bucket)
-- ARGV[3]  cost
-- Returns  {allowed, remaining, retry_ms, reset_ms}
local capacity = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local rate = capacity / window

-- Redis server time keeps every limiter replica on one clock.
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local st = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(st[1])
local ts = tonumber(st[2])
if tokens == nil or ts == nil then
  tokens = capacity
  ts = now
end
if now > ts then
  tokens = math.min(capacity, tokens + (now - ts) * rate)
  ts = now
end

local allowed = 0
local retry = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry = math.ceil((cost - tokens) / rate - 1e-9)
end
local reset = math.ceil((capacity - tokens) / rate - 1e-9)

redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(ts))
redis.call('PEXPIRE', KEYS[1], reset + 1000)
return {allowed, math.floor(tokens), retry, reset}
