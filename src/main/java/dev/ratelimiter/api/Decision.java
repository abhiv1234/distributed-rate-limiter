package dev.ratelimiter.api;

/**
 * The result of a rate limit check.
 *
 * @param allowed          whether the permits were granted
 * @param limit            the configured limit (capacity for token bucket)
 * @param remaining        permits still available right now (never negative)
 * @param retryAfterMillis when denied: how long until the same request would succeed; 0 when allowed
 * @param resetAfterMillis how long until the quota is fully restored
 * @param degraded         true if the decision was made by a fallback path (Redis unavailable)
 */
public record Decision(boolean allowed, long limit, long remaining, long retryAfterMillis,
                       long resetAfterMillis, boolean degraded) {

    public Decision {
        remaining = Math.max(0, remaining);
        retryAfterMillis = Math.max(0, retryAfterMillis);
        resetAfterMillis = Math.max(0, resetAfterMillis);
    }

    public static Decision allow(long limit, long remaining, long resetAfterMillis) {
        return new Decision(true, limit, remaining, 0, resetAfterMillis, false);
    }

    public static Decision deny(long limit, long remaining, long retryAfterMillis, long resetAfterMillis) {
        return new Decision(false, limit, remaining, retryAfterMillis, resetAfterMillis, false);
    }

    /** Used when the backing store is down and the failure mode is "fail open". */
    public static Decision failOpen(long limit) {
        return new Decision(true, limit, limit, 0, 0, true);
    }

    /** Used when the backing store is down and the failure mode is "fail closed". */
    public static Decision failClosed(long limit, long retryAfterMillis) {
        return new Decision(false, limit, 0, retryAfterMillis, retryAfterMillis, true);
    }

    public Decision asDegraded() {
        return degraded ? this : new Decision(allowed, limit, remaining, retryAfterMillis, resetAfterMillis, true);
    }
}
