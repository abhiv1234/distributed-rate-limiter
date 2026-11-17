package dev.ratelimiter.resilience;

import dev.ratelimiter.api.Clock;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Minimal circuit breaker guarding the Redis call.
 *
 * <pre>
 *  CLOSED --(N consecutive failures)--> OPEN --(cooldown elapsed)--> HALF_OPEN
 *     ^                                   ^                              |
 *     +----------(probe succeeds)---------+-------(probe fails)----------+
 * </pre>
 *
 * While OPEN, callers skip Redis entirely, so a dead Redis costs zero added latency instead of a
 * timeout per request. In HALF_OPEN exactly one probe request is let through.
 */
public final class CircuitBreaker {

    public enum State { CLOSED, OPEN, HALF_OPEN }

    private final int failureThreshold;
    private final long openMicros;
    private final Clock clock;

    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicInteger consecutiveFailures = new AtomicInteger();
    private final AtomicBoolean probeInFlight = new AtomicBoolean();
    private volatile long openedAtMicros;

    public CircuitBreaker(int failureThreshold, long openMillis, Clock clock) {
        if (failureThreshold < 1) {
            throw new IllegalArgumentException("failureThreshold must be >= 1");
        }
        this.failureThreshold = failureThreshold;
        this.openMicros = openMillis * 1000L;
        this.clock = clock;
    }

    public boolean tryPass() {
        switch (state.get()) {
            case CLOSED:
                return true;
            case OPEN:
                if (clock.nowMicros() - openedAtMicros < openMicros) {
                    return false;
                }
                state.compareAndSet(State.OPEN, State.HALF_OPEN);
                return probeInFlight.compareAndSet(false, true);
            case HALF_OPEN:
            default:
                return probeInFlight.compareAndSet(false, true);
        }
    }

    public void onSuccess() {
        consecutiveFailures.set(0);
        state.set(State.CLOSED);
        probeInFlight.set(false);
    }

    public void onFailure() {
        if (state.get() == State.HALF_OPEN) {
            trip();
            return;
        }
        if (consecutiveFailures.incrementAndGet() >= failureThreshold) {
            trip();
        }
    }

    private void trip() {
        openedAtMicros = clock.nowMicros();
        state.set(State.OPEN);
        probeInFlight.set(false);
    }

    public State state() {
        return state.get();
    }
}
