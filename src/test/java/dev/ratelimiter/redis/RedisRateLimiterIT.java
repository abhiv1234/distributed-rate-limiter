package dev.ratelimiter.redis;

import dev.ratelimiter.api.Decision;
import dev.ratelimiter.api.RateLimitRule;
import dev.ratelimiter.api.RateLimiter;
import dev.ratelimiter.testing.RedisTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Runs the Lua scripts against a real Redis. Skipped when Redis isn't reachable. */
class RedisRateLimiterIT {

    private RedisPool pool;
    private String prefix;

    @BeforeEach
    void setUp() {
        pool = RedisTestSupport.poolOrSkip();
        prefix = RedisTestSupport.uniquePrefix();
    }

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.close();
        }
    }

    private RedisRateLimiter limiter(RateLimitRule rule) {
        return new RedisRateLimiter(rule, pool, prefix);
    }

    @Test
    void tokenBucketBurstDenyAndRetryAfter() {
        RedisRateLimiter l = limiter(RateLimitRule.tokenBucket("tb", 5, 10)); // 1 token / 100 ms
        for (int i = 0; i < 5; i++) {
            Decision d = l.tryAcquire("u1");
            assertTrue(d.allowed());
            assertEquals(4 - i, d.remaining());
        }
        Decision denied = l.tryAcquire("u1");
        assertFalse(denied.allowed());
        assertTrue(denied.retryAfterMillis() > 0 && denied.retryAfterMillis() <= 100,
                "retryAfter was " + denied.retryAfterMillis());
    }

    @Test
    void tokenBucketRefillsOverTime() throws InterruptedException {
        RedisRateLimiter l = limiter(RateLimitRule.tokenBucket("tb2", 2, 20)); // 1 token / 50 ms
        assertTrue(l.tryAcquire("u").allowed());
        assertTrue(l.tryAcquire("u").allowed());
        Decision denied = l.tryAcquire("u");
        assertFalse(denied.allowed());
        Thread.sleep(denied.retryAfterMillis() + 5);
        assertTrue(l.tryAcquire("u").allowed(), "a token must be back after retryAfter");
    }

    @Test
    void slidingWindowLogIsExact() throws InterruptedException {
        RedisRateLimiter l = limiter(RateLimitRule.slidingWindowLog("log", 3, 300));
        assertTrue(l.tryAcquire("u", 2).allowed());
        assertTrue(l.tryAcquire("u").allowed());
        Decision denied = l.tryAcquire("u");
        assertFalse(denied.allowed());
        assertTrue(denied.retryAfterMillis() > 0 && denied.retryAfterMillis() <= 300);
        Thread.sleep(denied.retryAfterMillis() + 5);
        assertTrue(l.tryAcquire("u", 2).allowed(), "the two oldest entries have slid out");
    }

    @Test
    void slidingWindowCounterLimitsWithinWindow() {
        RedisRateLimiter l = limiter(RateLimitRule.slidingWindowCounter("swc", 10, 60_000));
        int allowed = 0;
        for (int i = 0; i < 25; i++) {
            if (l.tryAcquire("u").allowed()) {
                allowed++;
            }
        }
        // The previous window is empty, so the estimate is exact here.
        assertEquals(10, allowed);
    }

    @Test
    void keysGetATtlSoIdleStateIsReclaimed() {
        RedisRateLimiter tb = limiter(RateLimitRule.tokenBucket("ttl1", 10, 1));
        RedisRateLimiter log = limiter(RateLimitRule.slidingWindowLog("ttl2", 10, 5000));
        RedisRateLimiter swc = limiter(RateLimitRule.slidingWindowCounter("ttl3", 10, 5000));
        tb.tryAcquire("k");
        log.tryAcquire("k");
        swc.tryAcquire("k");
        long[] ttls = pool.execute(c -> new long[]{
                (Long) c.call("PTTL", tb.redisKey("k")),
                (Long) c.call("PTTL", log.redisKey("k")),
                (Long) c.call("PTTL", swc.redisKey("k"))});
        assertTrue(ttls[0] > 9_000 && ttls[0] <= 11_000, "token bucket ttl " + ttls[0]);
        assertTrue(ttls[1] > 4_000 && ttls[1] <= 5_000, "log ttl " + ttls[1]);
        assertTrue(ttls[2] > 9_000 && ttls[2] <= 10_000, "counter ttl " + ttls[2]);
    }

    @Test
    void recoversWhenRedisForgetsTheScriptCache() {
        RedisRateLimiter l = limiter(RateLimitRule.tokenBucket("noscript", 5, 1));
        assertTrue(l.tryAcquire("u").allowed());
        pool.execute(c -> c.call("SCRIPT", "FLUSH")); // simulates a Redis restart / failover
        assertTrue(l.tryAcquire("u").allowed(), "EVALSHA -> NOSCRIPT -> EVAL fallback");
        assertEquals(2, l.tryAcquire("u").remaining());
    }

    /**
     * The core distributed guarantee: several independent "nodes" (separate pools and limiter
     * instances) hammering one key together never exceed the global limit.
     */
    @Test
    void globalLimitHoldsAcrossNodes() throws Exception {
        final int nodes = 4;
        final int threadsPerNode = 8;
        final int attemptsPerThread = 200;
        final long limit = 500;
        RateLimitRule rule = RateLimitRule.slidingWindowLog("global", limit, 60_000);

        List<RedisPool> pools = new ArrayList<>();
        List<RateLimiter> limiters = new ArrayList<>();
        for (int n = 0; n < nodes; n++) {
            RedisPool p = new RedisPool(RedisTestSupport.config().withPoolSize(threadsPerNode));
            pools.add(p);
            limiters.add(new RedisRateLimiter(rule, p, prefix));
        }
        AtomicInteger allowed = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        for (RateLimiter node : limiters) {
            for (int t = 0; t < threadsPerNode; t++) {
                Thread th = Thread.ofVirtual().start(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        return;
                    }
                    for (int i = 0; i < attemptsPerThread; i++) {
                        if (node.tryAcquire("tenant-42").allowed()) {
                            allowed.incrementAndGet();
                        }
                    }
                });
                threads.add(th);
            }
        }
        start.countDown();
        for (Thread th : threads) {
            th.join();
        }
        pools.forEach(RedisPool::close);
        assertEquals(limit, allowed.get(), "6400 attempts from 4 nodes; exactly the global limit may pass");
    }
}
