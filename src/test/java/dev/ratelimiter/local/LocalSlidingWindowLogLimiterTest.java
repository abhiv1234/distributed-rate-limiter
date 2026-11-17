package dev.ratelimiter.local;

import dev.ratelimiter.api.Decision;
import dev.ratelimiter.api.RateLimitRule;
import dev.ratelimiter.testing.ManualClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocalSlidingWindowLogLimiterTest {

    private ManualClock clock;
    private LocalSlidingWindowLogLimiter limiter;

    @BeforeEach
    void setUp() {
        clock = new ManualClock();
        limiter = new LocalSlidingWindowLogLimiter(RateLimitRule.slidingWindowLog("log", 5, 1000), clock);
    }

    @Test
    void exactLimitInAnyWindow() {
        for (int i = 0; i < 5; i++) {
            assertTrue(limiter.tryAcquire("k").allowed());
            clock.advanceMillis(100);
        }
        // t=500: the first admission (t=0) is still inside the 1 s window
        assertFalse(limiter.tryAcquire("k").allowed());
    }

    @Test
    void entriesSlideOutExactlyAfterTheWindow() {
        assertTrue(limiter.tryAcquire("k").allowed());      // t=0
        clock.advanceMillis(500);
        for (int i = 0; i < 4; i++) {
            assertTrue(limiter.tryAcquire("k").allowed());  // t=500
        }
        Decision denied = limiter.tryAcquire("k");
        assertFalse(denied.allowed());
        assertEquals(500, denied.retryAfterMillis(), "the t=0 entry leaves at t=1000");

        clock.advanceMillis(500);                            // t=1000
        assertTrue(limiter.tryAcquire("k").allowed());
        assertFalse(limiter.tryAcquire("k").allowed());
    }

    @Test
    void multiPermitRetryAfterWaitsForEnoughEntries() {
        for (int i = 0; i < 5; i++) {
            limiter.tryAcquire("k");
            clock.advanceMillis(100);
        }
        // t=500, entries at 0,100,200,300,400. Need 3 free => the entry at t=200 must expire (t=1200).
        Decision d = limiter.tryAcquire("k", 3);
        assertFalse(d.allowed());
        assertEquals(700, d.retryAfterMillis());
        clock.advanceMillis(700);
        assertTrue(limiter.tryAcquire("k", 3).allowed());
    }

    @Test
    void ringBufferGrowsAndWrapsCorrectly() {
        LocalSlidingWindowLogLimiter big = new LocalSlidingWindowLogLimiter(
                RateLimitRule.slidingWindowLog("big", 100, 1000), clock);
        for (int round = 0; round < 5; round++) {
            for (int i = 0; i < 100; i++) {
                assertTrue(big.tryAcquire("k").allowed(), "round " + round + " req " + i);
            }
            assertFalse(big.tryAcquire("k").allowed());
            clock.advanceMillis(1000);
        }
    }

    @Test
    void idleKeysAreEvicted() {
        limiter.tryAcquire("a");
        limiter.tryAcquire("b");
        assertEquals(2, limiter.trackedKeys());
        clock.advanceMillis(999);
        assertEquals(0, limiter.evictIdle());
        clock.advanceMillis(1);
        assertEquals(2, limiter.evictIdle());
        assertEquals(0, limiter.trackedKeys());
        assertTrue(limiter.tryAcquire("a").allowed(), "evicted key starts fresh");
    }
}
