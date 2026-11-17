-- Token bucket, executed atomically by Redis.
-- KEYS[1] = bucket key (hash: tokens, ts)
-- ARGV[1] = capacity, ARGV[2] = refill tokens per second, ARGV[3] = permits requested
-- Returns {allowed (0|1), remaining, retry_after_us, reset_after_us}
--
-- Time comes from the Redis server (TIME), not from the callers, so N app servers with
-- skewed clocks still share one consistent timeline.
if redis.replicate_commands then pcall(redis.replicate_commands) end -- Redis < 7: allow TIME before writes

local capacity  = tonumber(ARGV[1])
local rate      = tonumber(ARGV[2])               -- tokens per second
local requested = tonumber(ARGV[3])

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])

local state  = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts     = tonumber(state[2])
if tokens == nil or ts == nil then
  tokens = capacity
  ts = now
end

local elapsed = now - ts
if elapsed < 0 then elapsed = 0 end            -- never mint tokens if the clock steps back
if now > ts then ts = now end
tokens = math.min(capacity, tokens + elapsed * rate / 1000000)

local allowed, retry = 0, 0
if tokens >= requested then
  tokens = tokens - requested
  allowed = 1
else
  retry = math.ceil((requested - tokens) * 1000000 / rate)
end
local reset = math.ceil((capacity - tokens) * 1000000 / rate)

-- Format explicitly: Lua's default number->string keeps only 14 significant digits,
-- which would truncate a microsecond epoch timestamp (16 digits).
redis.call('HSET', KEYS[1], 'tokens', string.format('%.17g', tokens), 'ts', string.format('%.0f', ts))
-- After a full refill, a missing key is identical to a full bucket, so let Redis reclaim it.
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity * 1000 / rate) + 1000)

return {allowed, math.floor(tokens), retry, reset}
