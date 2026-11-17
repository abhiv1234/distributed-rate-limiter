package dev.ratelimiter.redis;

import dev.ratelimiter.api.RateLimiterException;

/** Redis answered with an error reply (e.g. NOSCRIPT, WRONGTYPE). The connection is still usable. */
public class RedisReplyException extends RateLimiterException {
    public RedisReplyException(String message) {
        super(message);
    }

    public boolean isNoScript() {
        return getMessage() != null && getMessage().startsWith("NOSCRIPT");
    }
}
