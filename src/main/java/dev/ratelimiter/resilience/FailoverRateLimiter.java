package dev.ratelimiter.resilience;

import dev.ratelimiter.api.Decision;
import dev.ratelimiter.api.RateLimitRule;
import dev.ratelimiter.api.RateLimiter;

import java.util.concurrent.atomic.LongAdder;

/**
 * Wraps a distributed limiter with a circuit breaker and a {@link FailureMode}.
 * Decisions made without Redis are flagged {@link Decision#degraded()} so they can be counted.
 */
public final class FailoverRateLimiter implements RateLimiter {

    private final RateLimiter primary;
    private final RateLimiter fallback;   // may be null unless mode == LOCAL_FALLBACK
    private final FailureMode mode;
    private final CircuitBreaker breaker;
    private final LongAdder primaryErrors = new LongAdder();
    private final LongAdder degradedDecisions = new LongAdder();

    public FailoverRateLimiter(RateLimiter primary, RateLimiter fallback, FailureMode mode, CircuitBreaker breaker) {
        if (mode == FailureMode.LOCAL_FALLBACK && fallback == null) {
            throw new IllegalArgumentException("LOCAL_FALLBACK requires a fallback limiter");
        }
        this.primary = primary;
        this.fallback = fallback;
        this.mode = mode;
        this.breaker = breaker;
    }

    @Override
    public RateLimitRule rule() {
        return primary.rule();
    }

    @Override
    public Decision tryAcquire(String key, int permits) {
        RateLimiter.validate(rule(), key, permits);
        if (breaker.tryPass()) {
            try {
                Decision d = primary.tryAcquire(key, permits);
                breaker.onSuccess();
                return d;
            } catch (RuntimeException e) { // RateLimiterException, or anything unexpected from the store
                primaryErrors.increment();
                breaker.onFailure();
            }
        }
        degradedDecisions.increment();
        return switch (mode) {
            case FAIL_OPEN -> Decision.failOpen(rule().limit());
            case FAIL_CLOSED -> Decision.failClosed(rule().limit(), 1000);
            case LOCAL_FALLBACK -> fallback.tryAcquire(key, Math.min(permits, (int) fallback.rule().limit())).asDegraded();
        };
    }

    public CircuitBreaker.State circuitState() {
        return breaker.state();
    }

    public long primaryErrors() {
        return primaryErrors.sum();
    }

    public long degradedDecisions() {
        return degradedDecisions.sum();
    }

    public FailureMode mode() {
        return mode;
    }

    public RateLimiter fallback() {
        return fallback;
    }
}
