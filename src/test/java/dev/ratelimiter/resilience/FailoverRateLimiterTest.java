package dev.ratelimiter.resilience;

import dev.ratelimiter.api.Decision;
import dev.ratelimiter.api.RateLimitRule;
import dev.ratelimiter.api.RateLimiter;
import dev.ratelimiter.api.RateLimiterException;
import dev.ratelimiter.local.LocalTokenBucketLimiter;
import dev.ratelimiter.testing.ManualClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FailoverRateLimiterTest {

    private static final RateLimitRule RULE = RateLimitRule.tokenBucket("api", 6, 1);

    /** A primary that can be switched between healthy and failing, and counts its calls. */
    static final class FlakyPrimary implements RateLimiter {
        final AtomicBoolean down = new AtomicBoolean();
        final AtomicInteger calls = new AtomicInteger();

        @Override
        public RateLimitRule rule() {
            return RULE;
        }

        @Override
        public Decision tryAcquire(String key, int permits) {
            calls.incrementAndGet();
            if (down.get()) {
                throw new RateLimiterException("redis down");
            }
            return Decision.allow(RULE.limit(), 5, 0);
        }
    }

    private ManualClock clock;
    private FlakyPrimary primary;

    @BeforeEach
    void setUp() {
        clock = new ManualClock();
        primary = new FlakyPrimary();
    }

    private FailoverRateLimiter limiter(FailureMode mode) {
        RateLimiter fallback = new LocalTokenBucketLimiter(RULE.dividedBy(3), clock); // 6/3 = 2 per node
        return new FailoverRateLimiter(primary, fallback, mode, new CircuitBreaker(3, 1000, clock));
    }

    @Test
    void healthyPrimaryIsUsedAndNotDegraded() {
        Decision d = limiter(FailureMode.FAIL_CLOSED).tryAcquire("k");
        assertTrue(d.allowed());
        assertFalse(d.degraded());
    }

    @Test
    void failOpenAllowsAndFlagsDegraded() {
        primary.down.set(true);
        Decision d = limiter(FailureMode.FAIL_OPEN).tryAcquire("k");
        assertTrue(d.allowed());
        assertTrue(d.degraded());
    }

    @Test
    void failClosedDeniesWithRetryAfter() {
        primary.down.set(true);
        Decision d = limiter(FailureMode.FAIL_CLOSED).tryAcquire("k");
        assertFalse(d.allowed());
        assertTrue(d.degraded());
        assertTrue(d.retryAfterMillis() > 0);
    }

    @Test
    void localFallbackEnforcesPerNodeShareOfTheLimit() {
        primary.down.set(true);
        FailoverRateLimiter l = limiter(FailureMode.LOCAL_FALLBACK);
        assertTrue(l.tryAcquire("k").allowed());
        assertTrue(l.tryAcquire("k").allowed());
        Decision third = l.tryAcquire("k");
        assertFalse(third.allowed(), "fallback limit is 6 / 3 nodes = 2");
        assertTrue(third.degraded());
    }

    @Test
    void circuitOpensAfterThresholdAndStopsCallingRedis() {
        primary.down.set(true);
        FailoverRateLimiter l = limiter(FailureMode.FAIL_OPEN);
        for (int i = 0; i < 10; i++) {
            l.tryAcquire("k");
        }
        assertEquals(3, primary.calls.get(), "after 3 failures the breaker short-circuits");
        assertEquals(CircuitBreaker.State.OPEN, l.circuitState());
        assertEquals(10, l.degradedDecisions());
    }

    @Test
    void halfOpenProbeClosesCircuitWhenRedisRecovers() {
        primary.down.set(true);
        FailoverRateLimiter l = limiter(FailureMode.FAIL_OPEN);
        for (int i = 0; i < 3; i++) {
            l.tryAcquire("k");
        }
        assertEquals(CircuitBreaker.State.OPEN, l.circuitState());

        primary.down.set(false);
        clock.advanceMillis(999);
        assertTrue(l.tryAcquire("k").degraded(), "still open during cooldown");
        clock.advanceMillis(1);
        Decision probe = l.tryAcquire("k");
        assertFalse(probe.degraded(), "probe went to the primary");
        assertEquals(CircuitBreaker.State.CLOSED, l.circuitState());
    }

    @Test
    void failedProbeReopensCircuit() {
        primary.down.set(true);
        FailoverRateLimiter l = limiter(FailureMode.FAIL_OPEN);
        for (int i = 0; i < 3; i++) {
            l.tryAcquire("k");
        }
        clock.advanceMillis(1000);
        l.tryAcquire("k"); // probe fails
        assertEquals(CircuitBreaker.State.OPEN, l.circuitState());
        int calls = primary.calls.get();
        l.tryAcquire("k");
        assertEquals(calls, primary.calls.get(), "re-opened: no call until the next cooldown ends");
    }
}
