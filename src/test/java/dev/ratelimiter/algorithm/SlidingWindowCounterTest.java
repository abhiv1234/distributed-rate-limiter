package dev.ratelimiter.algorithm;

import dev.ratelimiter.api.Decision;
import dev.ratelimiter.api.RateLimitRule;
import dev.ratelimiter.local.LocalSlidingWindowCounterLimiter;
import dev.ratelimiter.testing.ManualClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SlidingWindowCounterTest {

    private static final long WINDOW_MS = 1000;
    private ManualClock clock;
    private LocalSlidingWindowCounterLimiter limiter;

    @BeforeEach
    void setUp() {
        clock = new ManualClock(0); // aligned to a window boundary
        limiter = new LocalSlidingWindowCounterLimiter(RateLimitRule.slidingWindowCounter("c", 10, WINDOW_MS), clock);
    }

    @Test
    void allowsLimitWithinOneWindow() {
        for (int i = 0; i < 10; i++) {
            assertTrue(limiter.tryAcquire("k").allowed());
        }
        assertFalse(limiter.tryAcquire("k").allowed());
    }

    @Test
    void previousWindowIsWeightedByOverlap() {
        for (int i = 0; i < 10; i++) {
            limiter.tryAcquire("k");
        }
        // 25% into the next window, the previous window still counts for 75%: 7.5 used, 2 more fit.
        clock.set(1_250_000);
        assertTrue(limiter.tryAcquire("k").allowed());
        assertTrue(limiter.tryAcquire("k").allowed());
        assertFalse(limiter.tryAcquire("k").allowed(), "7.5 + 2 + 1 > 10");
    }

    @Test
    void preventsTheFixedWindowBoundaryBurst() {
        // A fixed window would allow 10 at t=999ms and 10 more at t=1000ms (20 in 1 ms).
        clock.set(999_000);
        for (int i = 0; i < 10; i++) {
            assertTrue(limiter.tryAcquire("k").allowed());
        }
        clock.set(1_000_000);
        assertFalse(limiter.tryAcquire("k").allowed());
    }

    @Test
    void stateExpiresAfterTwoIdleWindows() {
        for (int i = 0; i < 10; i++) {
            limiter.tryAcquire("k");
        }
        clock.set(2_000_000);
        Decision d = limiter.tryAcquire("k");
        assertTrue(d.allowed());
        assertEquals(9, d.remaining());
    }

    @Test
    void retryAfterLeadsToSuccess() {
        for (int i = 0; i < 10; i++) {
            limiter.tryAcquire("k");
        }
        clock.set(1_100_000);
        Decision denied = limiter.tryAcquire("k", 2);
        // 10*(1-0.1) = 9 used; need weight <= 8 => elapsed >= 20% => wait ~100 ms
        assertFalse(denied.allowed());
        assertEquals(100, denied.retryAfterMillis());
        clock.advanceMillis(denied.retryAfterMillis());
        assertTrue(limiter.tryAcquire("k", 2).allowed());
    }

    @Test
    void retryAfterSpansIntoNextWindowWhenCurrentWindowIsFull() {
        clock.set(500_000);
        for (int i = 0; i < 10; i++) {
            limiter.tryAcquire("k");
        }
        Decision denied = limiter.tryAcquire("k");
        assertFalse(denied.allowed());
        // Window ends in 500 ms; then 10 decays, need 10*(1-f)+1 <= 10 => f >= 0.1 => +100 ms
        assertEquals(600, denied.retryAfterMillis());
        clock.advanceMillis(denied.retryAfterMillis());
        assertTrue(limiter.tryAcquire("k").allowed());
    }
}
