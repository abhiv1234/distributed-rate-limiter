package dev.ratelimiter.api;

/**
 * A rate limiter for a single {@link RateLimitRule}, partitioned by key
 * (user id, API key, IP address, tenant ...).
 *
 * <p>Implementations must be thread-safe.
 */
public interface RateLimiter extends AutoCloseable {

    /** The rule this limiter enforces. */
    RateLimitRule rule();

    /**
     * Atomically tries to take {@code permits} for {@code key}. Never blocks waiting for permits;
     * a denied decision carries {@link Decision#retryAfterMillis()} instead.
     *
     * @throws IllegalArgumentException if permits &lt; 1 or permits &gt; rule limit
     * @throws RateLimiterException     if the backing store cannot be reached
     */
    Decision tryAcquire(String key, int permits);

    default Decision tryAcquire(String key) {
        return tryAcquire(key, 1);
    }

    @Override
    default void close() {
    }

    /** Shared argument validation. */
    static void validate(RateLimitRule rule, String key, int permits) {
        if (key == null || key.isEmpty() || key.length() > 256) {
            throw new IllegalArgumentException("key must be 1..256 chars");
        }
        if (permits < 1) {
            throw new IllegalArgumentException("permits must be >= 1");
        }
        if (permits > rule.limit()) {
            throw new IllegalArgumentException("permits (" + permits + ") exceed rule limit (" + rule.limit() + ")");
        }
    }
}
