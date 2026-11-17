package dev.ratelimiter.local;

import dev.ratelimiter.api.RateLimitRule;
import dev.ratelimiter.api.RateLimiter;
import dev.ratelimiter.testing.ManualClock;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Many threads race on one hot key with time frozen: exactly {@code limit} requests may win.
 * Catches lost updates in the CAS loops and missing locking in the log.
 */
class LocalConcurrencyTest {

    private static final int THREADS = 16;
    private static final int ATTEMPTS_PER_THREAD = 5_000;
    private static final int LIMIT = 10_000;

    @Test
    void tokenBucketAdmitsExactlyCapacityUnderContention() throws Exception {
        assertExactlyLimitAdmitted(new LocalTokenBucketLimiter(RateLimitRule.tokenBucket("tb", LIMIT, 1), new ManualClock()));
    }

    @Test
    void slidingWindowCounterAdmitsExactlyLimitUnderContention() throws Exception {
        assertExactlyLimitAdmitted(new LocalSlidingWindowCounterLimiter(
                RateLimitRule.slidingWindowCounter("swc", LIMIT, 60_000), new ManualClock(0)));
    }

    @Test
    void slidingWindowLogAdmitsExactlyLimitUnderContention() throws Exception {
        assertExactlyLimitAdmitted(new LocalSlidingWindowLogLimiter(
                RateLimitRule.slidingWindowLog("swl", LIMIT, 60_000), new ManualClock()));
    }

    private static void assertExactlyLimitAdmitted(RateLimiter limiter) throws Exception {
        AtomicInteger allowed = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < THREADS; t++) {
            Thread th = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    return;
                }
                for (int i = 0; i < ATTEMPTS_PER_THREAD; i++) {
                    if (limiter.tryAcquire("hot").allowed()) {
                        allowed.incrementAndGet();
                    }
                }
            });
            th.start();
            threads.add(th);
        }
        start.countDown();
        for (Thread th : threads) {
            th.join();
        }
        assertEquals(LIMIT, allowed.get(), limiter.rule().algorithm() + " admitted the wrong number of requests");
    }
}
