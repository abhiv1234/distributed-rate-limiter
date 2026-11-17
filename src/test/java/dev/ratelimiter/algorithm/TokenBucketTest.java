package dev.ratelimiter.algorithm;

import dev.ratelimiter.api.Decision;
import dev.ratelimiter.api.RateLimitRule;
import dev.ratelimiter.local.LocalTokenBucketLimiter;
import dev.ratelimiter.testing.ManualClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TokenBucketTest {

    private ManualClock clock;
    private LocalTokenBucketLimiter limiter;

    @BeforeEach
    void setUp() {
        clock = new ManualClock();
        // capacity 10, refill 5 tokens/s => one token every 200 ms
        limiter = new LocalTokenBucketLimiter(RateLimitRule.tokenBucket("t", 10, 5), clock);
    }

    @Test
    void allowsBurstUpToCapacityThenDenies() {
        for (int i = 0; i < 10; i++) {
            Decision d = limiter.tryAcquire("k");
            assertTrue(d.allowed(), "request " + i);
            assertEquals(9 - i, d.remaining());
        }
        Decision denied = limiter.tryAcquire("k");
        assertFalse(denied.allowed());
        assertEquals(0, denied.remaining());
        assertEquals(200, denied.retryAfterMillis(), "one token takes 200 ms at 5/s");
    }

    @Test
    void refillsContinuouslyAtConfiguredRate() {
        for (int i = 0; i < 10; i++) {
            limiter.tryAcquire("k");
        }
        clock.advanceMillis(199);
        assertFalse(limiter.tryAcquire("k").allowed(), "not quite one token yet");
        clock.advanceMillis(1);
        assertTrue(limiter.tryAcquire("k").allowed(), "exactly one token after 200 ms");
        assertFalse(limiter.tryAcquire("k").allowed());
    }

    @Test
    void neverRefillsAboveCapacity() {
        clock.advanceMillis(60_000);
        Decision d = limiter.tryAcquire("k");
        assertTrue(d.allowed());
        assertEquals(9, d.remaining());
    }

    @Test
    void retryAfterIsHonestForMultiPermitRequests() {
        for (int i = 0; i < 10; i++) {
            limiter.tryAcquire("k");
        }
        Decision d = limiter.tryAcquire("k", 3);
        assertFalse(d.allowed());
        assertEquals(600, d.retryAfterMillis());
        clock.advanceMillis(d.retryAfterMillis());
        assertTrue(limiter.tryAcquire("k", 3).allowed(), "retrying after retryAfter must succeed");
    }

    @Test
    void deniedRequestsDoNotConsumeTokens() {
        for (int i = 0; i < 8; i++) {
            limiter.tryAcquire("k");
        }
        assertFalse(limiter.tryAcquire("k", 5).allowed());
        assertTrue(limiter.tryAcquire("k", 2).allowed(), "the 2 remaining tokens are still there");
    }

    @Test
    void keysAreIndependent() {
        for (int i = 0; i < 10; i++) {
            limiter.tryAcquire("a");
        }
        assertFalse(limiter.tryAcquire("a").allowed());
        assertTrue(limiter.tryAcquire("b").allowed());
    }

    @Test
    void clockGoingBackwardsNeverMintsTokens() {
        for (int i = 0; i < 10; i++) {
            limiter.tryAcquire("k");
        }
        clock.advanceMicros(-5_000_000);
        assertFalse(limiter.tryAcquire("k").allowed());
    }

    @Test
    void fractionalRefillRatesWork() {
        LocalTokenBucketLimiter slow = new LocalTokenBucketLimiter(RateLimitRule.tokenBucket("s", 1, 0.5), clock);
        assertTrue(slow.tryAcquire("k").allowed());
        Decision d = slow.tryAcquire("k");
        assertFalse(d.allowed());
        assertEquals(2000, d.retryAfterMillis());
    }

    @Test
    void rejectsInvalidArguments() {
        assertThrows(IllegalArgumentException.class, () -> limiter.tryAcquire("k", 0));
        assertThrows(IllegalArgumentException.class, () -> limiter.tryAcquire("k", 11));
        assertThrows(IllegalArgumentException.class, () -> limiter.tryAcquire("", 1));
        assertThrows(IllegalArgumentException.class, () -> RateLimitRule.tokenBucket("bad name!", 1, 1));
        assertThrows(IllegalArgumentException.class, () -> RateLimitRule.tokenBucket("x", 0, 1));
    }
}
