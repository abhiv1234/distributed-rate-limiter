package dev.ratelimiter.config;

import dev.ratelimiter.api.Clock;
import dev.ratelimiter.api.RateLimitRule;
import dev.ratelimiter.api.RateLimiter;
import dev.ratelimiter.local.LocalLimiterSupport;
import dev.ratelimiter.local.LocalSlidingWindowCounterLimiter;
import dev.ratelimiter.local.LocalSlidingWindowLogLimiter;
import dev.ratelimiter.local.LocalTokenBucketLimiter;
import dev.ratelimiter.redis.RedisPool;
import dev.ratelimiter.redis.RedisRateLimiter;
import dev.ratelimiter.resilience.CircuitBreaker;
import dev.ratelimiter.resilience.FailoverRateLimiter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Builds one limiter per configured rule and owns shared resources (Redis pool, eviction timer). */
public final class RateLimiterRegistry implements AutoCloseable {

    private final LimiterConfig config;
    private final Map<String, RateLimiter> limiters = new LinkedHashMap<>();
    private final List<LocalLimiterSupport<?>> localStores = new ArrayList<>();
    private final RedisPool pool;
    private final ScheduledExecutorService janitor;

    public RateLimiterRegistry(LimiterConfig config) {
        this.config = config;
        this.pool = config.mode() == LimiterConfig.Mode.REDIS ? new RedisPool(config.redis()) : null;
        for (RateLimitRule rule : config.rules()) {
            limiters.put(rule.name(), build(rule));
        }
        janitor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "rl-evictor");
            t.setDaemon(true);
            return t;
        });
        janitor.scheduleWithFixedDelay(this::evictIdle, 30, 30, TimeUnit.SECONDS);
    }

    private RateLimiter build(RateLimitRule rule) {
        if (pool == null) {
            return local(rule);
        }
        RateLimiter fallback = local(rule.dividedBy(config.expectedNodes()));
        CircuitBreaker breaker = new CircuitBreaker(config.breakerFailureThreshold(),
                config.breakerOpenMillis(), Clock.monotonic());
        return new FailoverRateLimiter(new RedisRateLimiter(rule, pool), fallback, config.failureMode(), breaker);
    }

    private RateLimiter local(RateLimitRule rule) {
        RateLimiter l = switch (rule.algorithm()) {
            case TOKEN_BUCKET -> new LocalTokenBucketLimiter(rule);
            case SLIDING_WINDOW_LOG -> new LocalSlidingWindowLogLimiter(rule);
            case SLIDING_WINDOW_COUNTER -> new LocalSlidingWindowCounterLimiter(rule);
        };
        localStores.add((LocalLimiterSupport<?>) l);
        return l;
    }

    public Optional<RateLimiter> get(String rule) {
        return Optional.ofNullable(limiters.get(rule));
    }

    public Collection<RateLimiter> all() {
        return limiters.values();
    }

    public LimiterConfig config() {
        return config;
    }

    /** Null in LOCAL mode. */
    public RedisPool pool() {
        return pool;
    }

    public int evictIdle() {
        int n = 0;
        for (LocalLimiterSupport<?> s : localStores) {
            n += s.evictIdle();
        }
        return n;
    }

    public int trackedLocalKeys() {
        return localStores.stream().mapToInt(LocalLimiterSupport::trackedKeys).sum();
    }

    @Override
    public void close() {
        janitor.shutdownNow();
        if (pool != null) {
            pool.close();
        }
    }
}
