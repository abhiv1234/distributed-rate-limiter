package dev.ratelimiter.algorithm;

import dev.ratelimiter.api.Decision;
import dev.ratelimiter.api.RateLimitRule;

/**
 * Sliding window counter as a pure state transition.
 *
 * <p>Time is cut into fixed windows. The count in the sliding window ending "now" is estimated as
 * <pre>
 *   estimate = previousCount * (1 - elapsedFractionOfCurrentWindow) + currentCount
 * </pre>
 * which assumes requests in the previous window were evenly spread. Memory is O(1) per key
 * (two counters and a window index), versus O(limit) for the exact log.
 *
 * <p>Mirrored in {@code src/main/resources/lua/sliding_window_counter.lua}.
 */
public final class SlidingWindowCounter {

    private SlidingWindowCounter() {
    }

    public record State(long windowIndex, long current, long previous) {
    }

    public record Result(State next, Decision decision) {
    }

    public static Result tryAcquire(State state, long nowMicros, RateLimitRule rule, int permits) {
        final long windowMicros = rule.windowMillis() * 1000L;
        final long limit = rule.limit();

        long idx = Math.floorDiv(nowMicros, windowMicros);
        long current;
        long previous;
        if (state == null || idx >= state.windowIndex() + 2) {
            current = 0;            // no state, or idle for 2+ windows: everything has slid out
            previous = 0;
        } else if (idx == state.windowIndex() + 1) {
            previous = state.current();  // roll forward by one window
            current = 0;
        } else {
            // Same window, or the clock stepped backwards: keep the newest state we know about.
            idx = state.windowIndex();
            current = state.current();
            previous = state.previous();
        }

        long windowStart = idx * windowMicros;
        long windowEnd = windowStart + windowMicros;
        double elapsedFraction = Math.min(1.0, Math.max(0.0, (nowMicros - windowStart) / (double) windowMicros));
        double estimate = previous * (1.0 - elapsedFraction) + current;
        long untilWindowEnd = Math.max(0, windowEnd - nowMicros);

        if (estimate + permits <= limit) {
            current += permits;
            long remaining = (long) Math.floor(limit - estimate - permits);
            long reset = untilWindowEnd + windowMicros; // `current` decays to zero at the end of the next window
            return new Result(new State(idx, current, previous),
                    Decision.allow(limit, remaining, Micros.ceilMillis(reset)));
        }

        long retryMicros;
        long slack = limit - current - permits;
        if (slack >= 0 && previous > 0) {
            // Wait until the previous window's weight has decayed enough within this window.
            double neededFraction = 1.0 - (double) slack / previous;
            retryMicros = (long) Math.ceil((neededFraction - elapsedFraction) * windowMicros);
        } else {
            // Even with `previous` fully decayed we'd be over: wait for the next window, where
            // today's `current` becomes the decaying `previous`.
            double neededFraction = current == 0 ? 0.0 : Math.max(0.0, 1.0 - (double) (limit - permits) / current);
            retryMicros = untilWindowEnd + (long) Math.ceil(neededFraction * windowMicros);
        }
        long remaining = (long) Math.floor(Math.max(0, limit - estimate));
        long reset = untilWindowEnd + (current > 0 ? windowMicros : 0);
        return new Result(new State(idx, current, previous),
                Decision.deny(limit, remaining, Micros.ceilMillis(Math.max(1, retryMicros)),
                        Micros.ceilMillis(reset)));
    }
}
