package dev.ratelimiter.api;

/** The rate limiting algorithms this library implements. */
public enum Algorithm {
    /**
     * Bucket of {@code limit} tokens refilled continuously at {@code refillPerSecond}.
     * Allows bursts up to capacity, then smooths to the refill rate. O(1) memory per key.
     */
    TOKEN_BUCKET,

    /**
     * Exact sliding window: stores one timestamp per admitted request.
     * Precise, but memory is O(limit) per key.
     */
    SLIDING_WINDOW_LOG,

    /**
     * Approximate sliding window: weights the previous fixed window's count by how much of it
     * still overlaps the sliding window. O(1) memory per key, small bounded error.
     */
    SLIDING_WINDOW_COUNTER
}
