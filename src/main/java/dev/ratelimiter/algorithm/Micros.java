package dev.ratelimiter.algorithm;

/** Time unit helpers. All algorithm arithmetic is done in microseconds. */
public final class Micros {
    private Micros() {
    }

    /** Microseconds to milliseconds, rounding up: never tell a client to retry too early. */
    public static long ceilMillis(long micros) {
        if (micros <= 0) {
            return 0;
        }
        return (micros + 999) / 1000;
    }
}
