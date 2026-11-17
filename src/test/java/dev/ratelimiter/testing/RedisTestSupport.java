package dev.ratelimiter.testing;

import dev.ratelimiter.redis.RedisConfig;
import dev.ratelimiter.redis.RedisPool;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests run against a real Redis at REDIS_HOST:REDIS_PORT (default localhost:6379)
 * and are skipped, not failed, when none is reachable. CI provides one as a service container.
 */
public final class RedisTestSupport {
    private RedisTestSupport() {
    }

    public static RedisConfig config() {
        String host = System.getenv().getOrDefault("REDIS_HOST", "localhost");
        int port = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
        return new RedisConfig(host, port, System.getenv("REDIS_PASSWORD"), 16, 500, 1000, 1000);
    }

    /** Returns a pool to a reachable Redis or aborts the test as skipped. */
    public static RedisPool poolOrSkip() {
        RedisPool pool = new RedisPool(config());
        boolean up = pool.ping();
        if (!up) {
            pool.close();
        }
        assumeTrue(up, "Redis not reachable at " + config().host() + ":" + config().port() + "; skipping");
        return pool;
    }

    /** Unique key prefix per test run so tests never see each other's state. */
    public static String uniquePrefix() {
        return "rltest-" + Long.toHexString(System.nanoTime());
    }
}
