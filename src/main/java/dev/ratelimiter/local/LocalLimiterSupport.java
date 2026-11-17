package dev.ratelimiter.local;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Per-key state storage shared by the local limiters, with idle-key eviction so that
 * high-cardinality keys (e.g. client IPs) don't grow memory without bound.
 *
 * <p>Eviction only removes state that is already equivalent to "no state" (a full bucket, an empty
 * window). A request racing with eviction may act on the just-removed entry; the worst case is that
 * one request against an idle key is counted against a fresh bucket instead of an identical full
 * one, so the race is benign.
 */
public abstract class LocalLimiterSupport<S> {

    protected final ConcurrentMap<String, S> states = new ConcurrentHashMap<>();

    protected abstract boolean isIdle(S state, long nowMicros);

    /** Removes idle keys using the limiter's own clock. Returns the number of keys removed. */
    public abstract int evictIdle();

    protected int evictIdle(long nowMicros) {
        int[] removed = {0};
        for (String key : states.keySet()) {
            states.computeIfPresent(key, (k, s) -> {
                if (isIdle(s, nowMicros)) {
                    removed[0]++;
                    return null;
                }
                return s;
            });
        }
        return removed[0];
    }

    /** Number of keys currently tracked (for metrics and tests). */
    public int trackedKeys() {
        return states.size();
    }
}
