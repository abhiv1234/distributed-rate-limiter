package dev.ratelimiter.bench;

import dev.ratelimiter.api.RateLimitRule;
import dev.ratelimiter.api.RateLimiter;
import dev.ratelimiter.local.LocalTokenBucketLimiter;
import dev.ratelimiter.redis.RedisConfig;
import dev.ratelimiter.redis.RedisPool;
import dev.ratelimiter.redis.RedisRateLimiter;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * Demonstrates why a distributed limiter is needed, and that this one holds the global limit.
 *
 * <p>Simulates N application nodes (each with its own connection pool and limiter instance) whose
 * threads all hammer the same key as fast as they can. Compares:
 * <ol>
 *   <li><b>local</b>: each node enforces the limit in-process (what you get without shared state);</li>
 *   <li><b>redis</b>: all nodes share state through Redis + Lua.</li>
 * </ol>
 * For a token bucket the most any correct limiter may admit in {@code t} seconds is
 * {@code capacity + rate * t}.
 *
 * <pre>
 * java -cp target/classes dev.ratelimiter.bench.DistributedCorrectnessCheck --nodes 4 --threads 16 --seconds 10
 * </pre>
 */
public final class DistributedCorrectnessCheck {

    public static void main(String[] args) throws Exception {
        int nodes = 4;
        int threadsPerNode = 16;
        int seconds = 10;
        String host = "localhost";
        int port = 6379;
        for (int i = 0; i + 1 < args.length; i += 2) {
            switch (args[i]) {
                case "--nodes" -> nodes = Integer.parseInt(args[i + 1]);
                case "--threads" -> threadsPerNode = Integer.parseInt(args[i + 1]);
                case "--seconds" -> seconds = Integer.parseInt(args[i + 1]);
                case "--redis" -> {
                    String[] hp = args[i + 1].split(":");
                    host = hp[0];
                    port = hp.length > 1 ? Integer.parseInt(hp[1]) : 6379;
                }
                default -> throw new IllegalArgumentException("unknown option " + args[i]);
            }
        }
        RateLimitRule rule = RateLimitRule.tokenBucket("global", 100, 200); // burst 100, 200 req/s
        System.out.printf("Rule: token bucket, capacity=%d, refill=%.0f/s. %d nodes x %d threads, %ds, one shared key.%n%n",
                rule.limit(), rule.refillPerSecond(), nodes, threadsPerNode, seconds);
        System.out.println("| strategy | attempts    | admitted | max allowed | over-admission |");
        System.out.println("|----------|-------------|----------|-------------|----------------|");

        // 1. Local only: every node has its own idea of the limit.
        List<RateLimiter> local = new ArrayList<>();
        for (int n = 0; n < nodes; n++) {
            local.add(new LocalTokenBucketLimiter(rule));
        }
        report("local", rule, hammer(local, threadsPerNode, seconds));

        // 2. Shared state in Redis.
        List<RedisPool> pools = new ArrayList<>();
        List<RateLimiter> shared = new ArrayList<>();
        String prefix = "check-" + System.nanoTime();
        for (int n = 0; n < nodes; n++) {
            RedisPool p = new RedisPool(new RedisConfig(host, port, null, threadsPerNode, 1000, 2000, 2000));
            pools.add(p);
            shared.add(new RedisRateLimiter(rule, p, prefix));
        }
        try {
            report("redis", rule, hammer(shared, threadsPerNode, seconds));
        } finally {
            pools.forEach(RedisPool::close);
        }
    }

    record Run(long attempts, long admitted, double seconds) {
    }

    private static Run hammer(List<RateLimiter> nodes, int threadsPerNode, int seconds) throws Exception {
        LongAdder attempts = new LongAdder();
        LongAdder admitted = new LongAdder();
        AtomicBoolean stop = new AtomicBoolean();
        CountDownLatch go = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        for (RateLimiter node : nodes) {
            for (int t = 0; t < threadsPerNode; t++) {
                threads.add(Thread.ofPlatform().start(() -> {
                    try {
                        go.await();
                    } catch (InterruptedException e) {
                        return;
                    }
                    while (!stop.get()) {
                        attempts.increment();
                        if (node.tryAcquire("tenant-42").allowed()) {
                            admitted.increment();
                        }
                    }
                }));
            }
        }
        long start = System.nanoTime();
        go.countDown();
        Thread.sleep(seconds * 1000L);
        stop.set(true);
        for (Thread th : threads) {
            th.join();
        }
        return new Run(attempts.sum(), admitted.sum(), (System.nanoTime() - start) / 1e9);
    }

    private static void report(String name, RateLimitRule rule, Run r) {
        long max = (long) Math.floor(rule.limit() + rule.refillPerSecond() * r.seconds());
        double over = 100.0 * (r.admitted() - max) / max;
        System.out.printf(Locale.ROOT, "| %-8s | %,11d | %,8d | %,11d | %13s |%n", name, r.attempts(), r.admitted(), max,
                r.admitted() <= max ? "none" : String.format(Locale.ROOT, "+%.0f%%", over));
    }
}
