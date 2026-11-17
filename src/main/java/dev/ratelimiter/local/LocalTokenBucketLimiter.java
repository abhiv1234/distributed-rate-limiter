package dev.ratelimiter.local;

import dev.ratelimiter.algorithm.TokenBucket;
import dev.ratelimiter.api.Clock;
import dev.ratelimiter.api.Decision;
import dev.ratelimiter.api.RateLimitRule;
import dev.ratelimiter.api.RateLimiter;

import java.util.concurrent.atomic.AtomicReference;

/**
 * In-process token bucket. Lock-free: each key holds an {@link AtomicReference} to an immutable
 * {@link TokenBucket.State}; a request computes the next state and publishes it with a single CAS,
 * retrying if another thread won the race. No thread ever blocks another.
 */
public final class LocalTokenBucketLimiter extends LocalLimiterSupport<AtomicReference<TokenBucket.State>>
        implements RateLimiter {

    private final RateLimitRule rule;
    private final Clock clock;

    public LocalTokenBucketLimiter(RateLimitRule rule) {
        this(rule, Clock.monotonic());
    }

    public LocalTokenBucketLimiter(RateLimitRule rule, Clock clock) {
        if (rule.algorithm() != dev.ratelimiter.api.Algorithm.TOKEN_BUCKET) {
            throw new IllegalArgumentException("not a token bucket rule: " + rule);
        }
        this.rule = rule;
        this.clock = clock;
    }

    @Override
    public RateLimitRule rule() {
        return rule;
    }

    @Override
    public Decision tryAcquire(String key, int permits) {
        RateLimiter.validate(rule, key, permits);
        AtomicReference<TokenBucket.State> ref = states.computeIfAbsent(key, k -> new AtomicReference<>());
        while (true) {
            TokenBucket.State current = ref.get();
            TokenBucket.Result result = TokenBucket.tryAcquire(current, clock.nowMicros(), rule, permits);
            if (ref.compareAndSet(current, result.next())) {
                return result.decision();
            }
            Thread.onSpinWait();
        }
    }

    @Override
    protected boolean isIdle(AtomicReference<TokenBucket.State> ref, long nowMicros) {
        TokenBucket.State s = ref.get();
        // Once the bucket would have refilled completely, dropping the state is lossless.
        return s == null || nowMicros - s.lastRefillMicros() > rule.stateTtlMillis() * 1000L;
    }

    @Override
    public int evictIdle() {
        return evictIdle(clock.nowMicros());
    }
}
