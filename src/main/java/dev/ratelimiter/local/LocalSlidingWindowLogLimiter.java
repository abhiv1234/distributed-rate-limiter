package dev.ratelimiter.local;

import dev.ratelimiter.algorithm.Micros;
import dev.ratelimiter.api.Algorithm;
import dev.ratelimiter.api.Clock;
import dev.ratelimiter.api.Decision;
import dev.ratelimiter.api.RateLimitRule;
import dev.ratelimiter.api.RateLimiter;

import java.util.concurrent.locks.ReentrantLock;

/**
 * In-process exact sliding window. Each key keeps the timestamps of admitted permits in a growable
 * ring buffer of primitive longs (no boxing), bounded by the rule limit.
 *
 * <p>Uses a per-key {@link ReentrantLock} rather than CAS, because the state is a mutable buffer.
 * {@code ReentrantLock} (not {@code synchronized}) so virtual threads are never pinned.
 */
public final class LocalSlidingWindowLogLimiter extends LocalLimiterSupport<LocalSlidingWindowLogLimiter.Log>
        implements RateLimiter {

    private final RateLimitRule rule;
    private final Clock clock;
    private final long windowMicros;

    public LocalSlidingWindowLogLimiter(RateLimitRule rule) {
        this(rule, Clock.monotonic());
    }

    public LocalSlidingWindowLogLimiter(RateLimitRule rule, Clock clock) {
        if (rule.algorithm() != Algorithm.SLIDING_WINDOW_LOG) {
            throw new IllegalArgumentException("not a sliding window log rule: " + rule);
        }
        if (rule.limit() > 10_000_000) {
            throw new IllegalArgumentException("sliding window log limit too large (O(limit) memory); use SLIDING_WINDOW_COUNTER");
        }
        this.rule = rule;
        this.clock = clock;
        this.windowMicros = rule.windowMillis() * 1000L;
    }

    @Override
    public RateLimitRule rule() {
        return rule;
    }

    @Override
    public Decision tryAcquire(String key, int permits) {
        RateLimiter.validate(rule, key, permits);
        while (true) {
            Log log = states.computeIfAbsent(key, k -> new Log((int) Math.min(rule.limit(), 16)));
            log.lock.lock();
            try {
                if (log.evicted) {
                    continue; // lost a race with eviction; retry with a fresh entry
                }
                return log.tryAcquire(clock.nowMicros(), permits);
            } finally {
                log.lock.unlock();
            }
        }
    }

    @Override
    protected boolean isIdle(Log log, long nowMicros) {
        if (!log.lock.tryLock()) {
            return false; // in use right now, so clearly not idle
        }
        try {
            boolean idle = log.size == 0 || log.newest() <= nowMicros - windowMicros;
            if (idle) {
                log.evicted = true;
            }
            return idle;
        } finally {
            log.lock.unlock();
        }
    }

    @Override
    public int evictIdle() {
        return evictIdle(clock.nowMicros());
    }

    /** Ring buffer of admission timestamps, oldest at {@code head}. Guarded by {@code lock}. */
    final class Log {
        final ReentrantLock lock = new ReentrantLock();
        long[] buf;
        int head;
        int size;
        boolean evicted;

        Log(int initialCapacity) {
            buf = new long[Math.max(1, initialCapacity)];
        }

        long at(int i) {
            return buf[(head + i) % buf.length];
        }

        long newest() {
            return at(size - 1);
        }

        Decision tryAcquire(long now, int permits) {
            long limit = rule.limit();
            // 1. Drop timestamps that have slid out of the window.
            while (size > 0 && buf[head] <= now - windowMicros) {
                head = (head + 1) % buf.length;
                size--;
            }
            if (size + permits <= limit) {
                ensureCapacity(size + permits);
                for (int i = 0; i < permits; i++) {
                    buf[(head + size) % buf.length] = now;
                    size++;
                }
                long fullReset = newest() + windowMicros - now; // when the newest entry slides out
                return Decision.allow(limit, limit - size, Micros.ceilMillis(fullReset));
            }
            // 2. Denied: we need (size + permits - limit) of the oldest entries to expire.
            int mustExpire = (int) (size + permits - limit);
            long retry = at(mustExpire - 1) + windowMicros - now;
            long fullReset = newest() + windowMicros - now;
            return Decision.deny(limit, limit - size, Micros.ceilMillis(Math.max(1, retry)),
                    Micros.ceilMillis(fullReset));
        }

        private void ensureCapacity(int needed) {
            if (needed <= buf.length) {
                return;
            }
            int newCap = (int) Math.min(rule.limit(), Math.max(needed, (long) buf.length * 2));
            long[] next = new long[newCap];
            for (int i = 0; i < size; i++) {
                next[i] = at(i);
            }
            buf = next;
            head = 0;
        }
    }
}
