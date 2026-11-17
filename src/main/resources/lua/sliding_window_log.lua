-- Exact sliding window log, executed atomically by Redis.
-- KEYS[1] = sorted set: member = unique request id, score = admission time (us)
-- ARGV[1] = limit, ARGV[2] = window (ms), ARGV[3] = permits, ARGV[4] = unique request id
-- Returns {allowed (0|1), remaining, retry_after_us, reset_after_us}
if redis.replicate_commands then pcall(redis.replicate_commands) end

local limit     = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local window    = window_ms * 1000
local permits   = tonumber(ARGV[3])
local id        = ARGV[4]

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])

-- 1. Evict everything that slid out of the window: O(log n + evicted)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', string.format('%.0f', now - window))
local count = redis.call('ZCARD', KEYS[1])

if count + permits <= limit then
  local score = string.format('%.0f', now) -- full precision (see token_bucket.lua)
  for i = 1, permits do
    redis.call('ZADD', KEYS[1], score, id .. ':' .. i)
  end
  redis.call('PEXPIRE', KEYS[1], window_ms)
  return {1, limit - count - permits, 0, window}
end

-- 2. Denied: the (count + permits - limit)-th oldest entry must expire first.
local idx = count + permits - limit - 1
local entry = redis.call('ZRANGE', KEYS[1], idx, idx, 'WITHSCORES')
local retry = 1
if entry[2] then
  retry = math.max(1, tonumber(entry[2]) + window - now)
end
local newest = redis.call('ZRANGE', KEYS[1], -1, -1, 'WITHSCORES')
local reset = window
if newest[2] then reset = math.max(0, tonumber(newest[2]) + window - now) end
return {0, math.max(0, limit - count), retry, reset}
