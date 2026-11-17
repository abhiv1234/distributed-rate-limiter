package dev.ratelimiter.redis;

import dev.ratelimiter.algorithm.Micros;
import dev.ratelimiter.api.Decision;
import dev.ratelimiter.api.RateLimitRule;
import dev.ratelimiter.api.RateLimiter;
import dev.ratelimiter.api.RateLimiterException;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Distributed limiter: all state lives in Redis and every decision is one atomic Lua script call
 * (one network round trip). Any number of application nodes can share a limit.
 *
 * <p><b>Key layout</b>: {@code <prefix>:<rule>:<key>}. Each decision touches exactly one Redis key,
 * so the design works unchanged on Redis Cluster: keys spread across slots by their own hash.
 * (A hash tag such as {@code {rule}} would pin a whole rule to one shard and create a hot spot.)
 */
public final class RedisRateLimiter implements RateLimiter {

    private static final LuaScript TOKEN_BUCKET = LuaScript.fromClasspath("token_bucket");
    private static final LuaScript WINDOW_LOG = LuaScript.fromClasspath("sliding_window_log");
    private static final LuaScript WINDOW_COUNTER = LuaScript.fromClasspath("sliding_window_counter");

    private final RateLimitRule rule;
    private final RedisPool pool;
    private final String keyPrefix;
    // Unique-per-process prefix + counter gives collision-free sorted-set members for the log.
    private final String nodeId = UUID.randomUUID().toString().substring(0, 8);
    private final AtomicLong sequence = new AtomicLong();

    public RedisRateLimiter(RateLimitRule rule, RedisPool pool) {
        this(rule, pool, "rl");
    }

    public RedisRateLimiter(RateLimitRule rule, RedisPool pool, String keyPrefix) {
        this.rule = rule;
        this.pool = pool;
        this.keyPrefix = keyPrefix;
    }

    @Override
    public RateLimitRule rule() {
        return rule;
    }

    public String redisKey(String key) {
        return keyPrefix + ":" + rule.name() + ":" + key;
    }

    @Override
    public Decision tryAcquire(String key, int permits) {
        RateLimiter.validate(rule, key, permits);
        String redisKey = redisKey(key);
        String p = Integer.toString(permits);
        Object reply = pool.execute(conn -> switch (rule.algorithm()) {
            case TOKEN_BUCKET -> TOKEN_BUCKET.eval(conn, redisKey,
                    Long.toString(rule.limit()), Double.toString(rule.refillPerSecond()), p);
            case SLIDING_WINDOW_LOG -> WINDOW_LOG.eval(conn, redisKey,
                    Long.toString(rule.limit()), Long.toString(rule.windowMillis()), p,
                    nodeId + "-" + sequence.incrementAndGet());
            case SLIDING_WINDOW_COUNTER -> WINDOW_COUNTER.eval(conn, redisKey,
                    Long.toString(rule.limit()), Long.toString(rule.windowMillis()), p);
        });
        return toDecision(reply);
    }

    private Decision toDecision(Object reply) {
        if (!(reply instanceof List<?> list) || list.size() != 4) {
            throw new RateLimiterException("unexpected reply from limiter script: " + reply);
        }
        boolean allowed = asLong(list.get(0)) == 1L;
        long remaining = asLong(list.get(1));
        long retryMillis = Micros.ceilMillis(asLong(list.get(2)));
        long resetMillis = Micros.ceilMillis(asLong(list.get(3)));
        return allowed
                ? Decision.allow(rule.limit(), remaining, resetMillis)
                : Decision.deny(rule.limit(), remaining, retryMillis, resetMillis);
    }

    private static long asLong(Object o) {
        if (o instanceof Long l) {
            return l;
        }
        throw new RateLimiterException("expected integer in script reply, got: " + o);
    }
}
