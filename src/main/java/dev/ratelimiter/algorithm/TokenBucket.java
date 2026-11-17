package dev.ratelimiter.algorithm;

import dev.ratelimiter.api.Decision;
import dev.ratelimiter.api.RateLimitRule;

/**
 * Token bucket as a pure state transition: {@code (state, now) -> (state', decision)}.
 *
 * <p>Refill is computed lazily on access ("refill on read"): there is no background timer,
 * so idle keys cost nothing. The same arithmetic is implemented in
 * {@code src/main/resources/lua/token_bucket.lua} for the distributed mode, and both are
 * covered by the same tests.
 */
public final class TokenBucket {

    private TokenBucket() {
    }

    /** {@code tokens} is fractional so slow refill rates (e.g. 0.5/s) work. */
    public record State(double tokens, long lastRefillMicros) {
    }

    public record Result(State next, Decision decision) {
    }

    public static State full(RateLimitRule rule, long nowMicros) {
        return new State(rule.limit(), nowMicros);
    }

    public static Result tryAcquire(State state, long nowMicros, RateLimitRule rule, int permits) {
        final double capacity = rule.limit();
        final double rate = rule.refillPerSecond();
        // Multiply/divide by 1e6 rather than precomputing tokens-per-microsecond: keeps results
        // exact for "round" rates (e.g. 5/s -> exactly 200_000 us per token).

        State s = state == null ? full(rule, nowMicros) : state;
        // A clock that moves backwards must never mint tokens.
        long elapsed = Math.max(0, nowMicros - s.lastRefillMicros());
        long ts = Math.max(nowMicros, s.lastRefillMicros());
        double tokens = Math.min(capacity, s.tokens() + elapsed * rate / 1_000_000.0);

        if (tokens >= permits) {
            tokens -= permits;
            long resetMicros = (long) Math.ceil((capacity - tokens) * 1_000_000.0 / rate);
            return new Result(new State(tokens, ts),
                    Decision.allow(rule.limit(), (long) Math.floor(tokens), Micros.ceilMillis(resetMicros)));
        }
        long retryMicros = (long) Math.ceil((permits - tokens) * 1_000_000.0 / rate);
        long resetMicros = (long) Math.ceil((capacity - tokens) * 1_000_000.0 / rate);
        return new Result(new State(tokens, ts),
                Decision.deny(rule.limit(), (long) Math.floor(tokens), Micros.ceilMillis(retryMicros),
                        Micros.ceilMillis(resetMicros)));
    }
}
