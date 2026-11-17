package dev.ratelimiter.resilience;

/** What to do when the distributed store (Redis) is unavailable. */
public enum FailureMode {
    /** Allow everything. Availability first; right for most product APIs. */
    FAIL_OPEN,
    /** Deny everything. Safety first; right for login/OTP/payment abuse protection. */
    FAIL_CLOSED,
    /**
     * Keep limiting with an in-process limiter whose limit is the global limit divided by the
     * expected number of nodes. Approximate, but neither a free-for-all nor an outage.
     */
    LOCAL_FALLBACK
}
