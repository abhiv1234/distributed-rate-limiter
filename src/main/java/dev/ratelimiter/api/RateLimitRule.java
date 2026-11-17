package dev.ratelimiter.api;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * An immutable rate limit policy.
 *
 * <ul>
 *   <li>{@link Algorithm#TOKEN_BUCKET}: {@code limit} is the bucket capacity (max burst),
 *       {@code refillPerSecond} is the sustained rate. {@code windowMillis} is ignored.</li>
 *   <li>Sliding window algorithms: at most {@code limit} permits in any {@code windowMillis}.
 *       {@code refillPerSecond} is ignored.</li>
 * </ul>
 */
public record RateLimitRule(String name, Algorithm algorithm, long limit, long windowMillis, double refillPerSecond) {

    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9_.-]{1,64}");

    public RateLimitRule {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(algorithm, "algorithm");
        if (!NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("rule name must match " + NAME.pattern() + ": " + name);
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be > 0 for rule " + name);
        }
        if (algorithm == Algorithm.TOKEN_BUCKET) {
            if (!(refillPerSecond > 0) || Double.isInfinite(refillPerSecond)) {
                throw new IllegalArgumentException("refillPerSecond must be > 0 for token bucket rule " + name);
            }
        } else if (windowMillis <= 0) {
            throw new IllegalArgumentException("windowMillis must be > 0 for window rule " + name);
        }
    }

    public static RateLimitRule tokenBucket(String name, long capacity, double refillPerSecond) {
        return new RateLimitRule(name, Algorithm.TOKEN_BUCKET, capacity, 0, refillPerSecond);
    }

    public static RateLimitRule slidingWindowLog(String name, long limit, long windowMillis) {
        return new RateLimitRule(name, Algorithm.SLIDING_WINDOW_LOG, limit, windowMillis, 0);
    }

    public static RateLimitRule slidingWindowCounter(String name, long limit, long windowMillis) {
        return new RateLimitRule(name, Algorithm.SLIDING_WINDOW_COUNTER, limit, windowMillis, 0);
    }

    /** Returns a copy whose limit (and refill rate) is divided by {@code n}; used for per-node fallback. */
    public RateLimitRule dividedBy(int n) {
        if (n <= 1) {
            return this;
        }
        long l = Math.max(1, limit / n);
        double r = algorithm == Algorithm.TOKEN_BUCKET ? Math.max(refillPerSecond / n, 1e-9) : refillPerSecond;
        return new RateLimitRule(name, algorithm, l, windowMillis, r);
    }

    /** How long idle state for this rule must be kept before it is equivalent to "no state". */
    public long stateTtlMillis() {
        if (algorithm == Algorithm.TOKEN_BUCKET) {
            // Time for an empty bucket to refill completely; after that, missing state == full bucket.
            return (long) Math.ceil(limit / refillPerSecond * 1000.0);
        }
        // Sliding window counter needs the previous window too.
        return algorithm == Algorithm.SLIDING_WINDOW_COUNTER ? 2 * windowMillis : windowMillis;
    }
}
