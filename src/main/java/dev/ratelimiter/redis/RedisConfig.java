package dev.ratelimiter.redis;

/**
 * Connection settings. Timeouts are intentionally tight: a rate limiter sits on the hot path of
 * every request, so it is better to fail fast (and apply the failure mode) than to add latency.
 */
public record RedisConfig(String host, int port, String password, int poolSize,
                          int connectTimeoutMs, int readTimeoutMs, int borrowTimeoutMs) {

    public RedisConfig {
        if (poolSize < 1) {
            throw new IllegalArgumentException("poolSize must be >= 1");
        }
    }

    public static RedisConfig defaults(String host, int port) {
        return new RedisConfig(host, port, null, 32, 200, 100, 50);
    }

    public RedisConfig withPoolSize(int n) {
        return new RedisConfig(host, port, password, n, connectTimeoutMs, readTimeoutMs, borrowTimeoutMs);
    }
}
