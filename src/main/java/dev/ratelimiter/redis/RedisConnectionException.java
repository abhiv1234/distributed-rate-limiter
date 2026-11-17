package dev.ratelimiter.redis;

import dev.ratelimiter.api.RateLimiterException;

/** Redis could not be reached, timed out, or the connection broke. */
public class RedisConnectionException extends RateLimiterException {
    public RedisConnectionException(String message) {
        super(message);
    }

    public RedisConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
