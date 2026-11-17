package dev.ratelimiter.testing;

import dev.ratelimiter.api.Clock;

import java.util.concurrent.atomic.AtomicLong;

/** Deterministic clock for tests. */
public final class ManualClock implements Clock {
    private final AtomicLong micros;

    public ManualClock(long startMicros) {
        this.micros = new AtomicLong(startMicros);
    }

    public ManualClock() {
        this(1_000_000_000L);
    }

    @Override
    public long nowMicros() {
        return micros.get();
    }

    public void advanceMillis(long ms) {
        micros.addAndGet(ms * 1000L);
    }

    public void advanceMicros(long us) {
        micros.addAndGet(us);
    }

    public void set(long us) {
        micros.set(us);
    }
}
