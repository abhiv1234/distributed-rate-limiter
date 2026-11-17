package dev.ratelimiter.api;

/** Thrown when a limiter cannot make a decision, e.g. the backing store is unreachable. */
public class RateLimiterException extends RuntimeException {
    public RateLimiterException(String message) {
        super(message);
    }

    public RateLimiterException(String message, Throwable cause) {
        super(message, cause);
    }
}
