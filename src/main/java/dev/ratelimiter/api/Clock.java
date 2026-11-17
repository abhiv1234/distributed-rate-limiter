package dev.ratelimiter.api;

/**
 * Time source in microseconds. Local limiters use a monotonic clock so that wall-clock
 * adjustments (NTP steps) can never refill a bucket or reset a window early.
 * Tests inject a manual clock to make time deterministic.
 */
@FunctionalInterface
public interface Clock {

    long nowMicros();

    /** Monotonic clock based on {@link System#nanoTime()}; only differences are meaningful. */
    static Clock monotonic() {
        return () -> System.nanoTime() / 1_000L;
    }
}
