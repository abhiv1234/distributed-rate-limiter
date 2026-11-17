-- Approximate sliding window counter, executed atomically by Redis.
-- Mirrors dev.ratelimiter.algorithm.SlidingWindowCounter exactly.
-- KEYS[1] = hash: w (window index), c (current count), p (previous count)
-- ARGV[1] = limit, ARGV[2] = window (ms), ARGV[3] = permits
-- Returns {allowed (0|1), remaining, retry_after_us, reset_after_us}
if redis.replicate_commands then pcall(redis.replicate_commands) end

local limit     = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local window    = window_ms * 1000
local permits   = tonumber(ARGV[3])

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local idx = math.floor(now / window)

local s = redis.call('HMGET', KEYS[1], 'w', 'c', 'p')
local w, cur, prev = tonumber(s[1]), tonumber(s[2]), tonumber(s[3])
if w == nil or idx >= w + 2 then
  cur, prev = 0, 0
elseif idx == w + 1 then
  prev, cur = cur, 0
else
  idx = w           -- same window (or clock stepped back): keep newest state
end

local window_start = idx * window
local window_end   = window_start + window
local frac = (now - window_start) / window
if frac < 0 then frac = 0 elseif frac > 1 then frac = 1 end
local estimate = prev * (1 - frac) + cur
local until_end = math.max(0, window_end - now)

local allowed, retry, remaining, reset
if estimate + permits <= limit then
  cur = cur + permits
  allowed = 1
  retry = 0
  remaining = math.floor(limit - estimate - permits)
  reset = until_end + window
else
  allowed = 0
  local slack = limit - cur - permits
  if slack >= 0 and prev > 0 then
    retry = math.ceil(((1 - slack / prev) - frac) * window)
  else
    local needed = 0
    if cur > 0 then needed = math.max(0, 1 - (limit - permits) / cur) end
    retry = until_end + math.ceil(needed * window)
  end
  retry = math.max(1, retry)
  remaining = math.floor(math.max(0, limit - estimate))
  reset = until_end
  if cur > 0 then reset = reset + window end
end

redis.call('HSET', KEYS[1], 'w', string.format('%.0f', idx), 'c', string.format('%.0f', cur), 'p', string.format('%.0f', prev))
redis.call('PEXPIRE', KEYS[1], 2 * window_ms)
return {allowed, remaining, retry, reset}
