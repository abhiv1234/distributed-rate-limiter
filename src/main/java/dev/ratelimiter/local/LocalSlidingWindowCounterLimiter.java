package dev.ratelimiter.local;

import dev.ratelimiter.algorithm.SlidingWindowCounter;
import dev.ratelimiter.api.Algorithm;
import dev.ratelimiter.api.Clock;
import dev.ratelimiter.api.Decision;
import dev.ratelimiter.api.RateLimitRule;
import dev.ratelimiter.api.RateLimiter;

import java.util.concurrent.atomic.AtomicReference;

/** In-process sliding window counter; lock-free CAS on an immutable state, like the token bucket. */
public final class LocalSlidingWindowCounterLimiter
        extends LocalLimiterSupport<AtomicReference<SlidingWindowCounter.State>> implements RateLimiter {

    private final RateLimitRule rule;
    private final Clock clock;

    public LocalSlidingWindowCounterLimiter(RateLimitRule rule) {
        this(rule, Clock.monotonic());
    }

    public LocalSlidingWindowCounterLimiter(RateLimitRule rule, Clock clock) {
        if (rule.algorithm() != Algorithm.SLIDING_WINDOW_COUNTER) {
            throw new IllegalArgumentException("not a sliding window counter rule: " + rule);
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
        AtomicReference<SlidingWindowCounter.State> ref = states.computeIfAbsent(key, k -> new AtomicReference<>());
        while (true) {
            SlidingWindowCounter.State current = ref.get();
            SlidingWindowCounter.Result result = SlidingWindowCounter.tryAcquire(current, clock.nowMicros(), rule, permits);
            if (ref.compareAndSet(current, result.next())) {
                return result.decision();
            }
            Thread.onSpinWait();
        }
    }

    @Override
    protected boolean isIdle(AtomicReference<SlidingWindowCounter.State> ref, long nowMicros) {
        SlidingWindowCounter.State s = ref.get();
        long idx = Math.floorDiv(nowMicros, rule.windowMillis() * 1000L);
        return s == null || idx >= s.windowIndex() + 2;
    }

    @Override
    public int evictIdle() {
        return evictIdle(clock.nowMicros());
    }
}
