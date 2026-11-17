package dev.ratelimiter.bench;

import dev.ratelimiter.api.Algorithm;
import dev.ratelimiter.api.RateLimitRule;
import dev.ratelimiter.api.RateLimiter;
import dev.ratelimiter.local.LocalSlidingWindowCounterLimiter;
import dev.ratelimiter.local.LocalSlidingWindowLogLimiter;
import dev.ratelimiter.local.LocalTokenBucketLimiter;
import dev.ratelimiter.redis.RedisConfig;
import dev.ratelimiter.redis.RedisPool;
import dev.ratelimiter.redis.RedisRateLimiter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Throughput / latency benchmark for every algorithm, locally and against Redis.
 *
 * <pre>
 * java -cp target/classes dev.ratelimiter.bench.Benchmark \
 *      --mode local,redis --threads 1,8,32 --keys 1,10000 --seconds 5 --redis localhost:6379
 * </pre>
 *
 * Each configuration warms up (JIT) and then runs for a fixed time. Every 16th call is timed
 * individually for the latency percentiles so that timing overhead doesn't distort throughput.
 * {@code --keys 1} is the worst case (every thread contends on one hot key).
 *
 * <p>For publication-grade microbenchmarks use JMH; this harness exists so the project has zero
 * dependencies and the numbers in the README are reproducible with one command.
 */
public final class Benchmark {

    private static final int SAMPLE_EVERY = 16;

    public static void main(String[] args) throws Exception {
        Args a = Args.parse(args);
        System.out.printf("JVM %s, %d cores. warmup=%ds measure=%ds%n%n",
                Runtime.version(), Runtime.getRuntime().availableProcessors(), a.warmupSeconds, a.seconds);
        System.out.println("| mode  | algorithm              | threads | keys   | ops/sec      | p50 (us) | p99 (us) | p99.9 (us) |");
        System.out.println("|-------|------------------------|---------|--------|--------------|----------|----------|------------|");
        for (String mode : a.modes) {
            for (Algorithm alg : a.algorithms) {
                for (int threads : a.threads) {
                    for (int keys : a.keys) {
                        Result r = run(mode, alg, threads, keys, a);
                        System.out.printf(Locale.ROOT, "| %-5s | %-22s | %7d | %6d | %,12.0f | %8.1f | %8.1f | %10.1f |%n",
                                mode, alg, threads, keys, r.opsPerSec, r.p50, r.p99, r.p999);
                    }
                }
            }
        }
    }

    record Result(double opsPerSec, double p50, double p99, double p999) {
    }

    static Result run(String mode, Algorithm alg, int threads, int keys, Args a) throws Exception {
        RateLimitRule rule = switch (alg) {
            case TOKEN_BUCKET -> RateLimitRule.tokenBucket("bench_tb", 1000, 1000);
            case SLIDING_WINDOW_LOG -> RateLimitRule.slidingWindowLog("bench_swl", 1000, 1000);
            case SLIDING_WINDOW_COUNTER -> RateLimitRule.slidingWindowCounter("bench_swc", 1000, 1000);
        };
        RedisPool pool = null;
        RateLimiter limiter;
        if (mode.equals("redis")) {
            pool = new RedisPool(new RedisConfig(a.redisHost, a.redisPort, null, threads, 1000, 2000, 2000));
            limiter = new RedisRateLimiter(rule, pool, "bench-" + System.nanoTime());
        } else {
            limiter = switch (alg) {
                case TOKEN_BUCKET -> new LocalTokenBucketLimiter(rule);
                case SLIDING_WINDOW_LOG -> new LocalSlidingWindowLogLimiter(rule);
                case SLIDING_WINDOW_COUNTER -> new LocalSlidingWindowCounterLimiter(rule);
            };
        }
        String[] keyNames = new String[keys];
        for (int i = 0; i < keys; i++) {
            keyNames[i] = "user-" + i;
        }
        try {
            phase(limiter, keyNames, threads, a.warmupSeconds);   // let the JIT settle
            return phase(limiter, keyNames, threads, a.seconds);
        } finally {
            if (pool != null) {
                pool.close();
            }
        }
    }

    private static Result phase(RateLimiter limiter, String[] keys, int threads, int seconds) throws Exception {
        AtomicBoolean stop = new AtomicBoolean();
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        long[] ops = new long[threads];
        List<long[]> samples = new ArrayList<>();
        int[] sampleCounts = new int[threads];
        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int id = t;
            long[] buf = new long[1 << 20];
            samples.add(buf);
            Thread th = new Thread(() -> {
                ready.countDown();
                try {
                    go.await();
                } catch (InterruptedException e) {
                    return;
                }
                long n = 0;
                int s = 0;
                int k = id * 7919;
                while (!stop.get()) {
                    String key = keys[(k++ & Integer.MAX_VALUE) % keys.length];
                    if ((n & (SAMPLE_EVERY - 1)) == 0 && s < buf.length) {
                        long t0 = System.nanoTime();
                        limiter.tryAcquire(key);
                        buf[s++] = System.nanoTime() - t0;
                    } else {
                        limiter.tryAcquire(key);
                    }
                    n++;
                }
                ops[id] = n;
                sampleCounts[id] = s;
            }, "bench-" + t);
            th.start();
            workers.add(th);
        }
        ready.await();
        long start = System.nanoTime();
        go.countDown();
        Thread.sleep(seconds * 1000L);
        stop.set(true);
        for (Thread th : workers) {
            th.join();
        }
        double elapsed = (System.nanoTime() - start) / 1e9;

        long total = Arrays.stream(ops).sum();
        int count = Arrays.stream(sampleCounts).sum();
        long[] all = new long[count];
        int pos = 0;
        for (int t = 0; t < threads; t++) {
            System.arraycopy(samples.get(t), 0, all, pos, sampleCounts[t]);
            pos += sampleCounts[t];
        }
        Arrays.sort(all);
        return new Result(total / elapsed, pct(all, 0.50), pct(all, 0.99), pct(all, 0.999));
    }

    private static double pct(long[] sorted, double q) {
        if (sorted.length == 0) {
            return 0;
        }
        int i = (int) Math.min(sorted.length - 1, Math.ceil(q * sorted.length) - 1);
        return sorted[Math.max(0, i)] / 1000.0;
    }

    static final class Args {
        List<String> modes = List.of("local");
        List<Algorithm> algorithms = List.of(Algorithm.values());
        int[] threads = {1, 8};
        int[] keys = {1, 10_000};
        int seconds = 5;
        int warmupSeconds = 2;
        String redisHost = "localhost";
        int redisPort = 6379;

        static Args parse(String[] argv) {
            Args a = new Args();
            for (int i = 0; i + 1 < argv.length; i += 2) {
                String v = argv[i + 1];
                switch (argv[i]) {
                    case "--mode" -> a.modes = List.of(v.split(","));
                    case "--algorithm" -> a.algorithms = Arrays.stream(v.split(","))
                            .map(s -> Algorithm.valueOf(s.trim().toUpperCase(Locale.ROOT))).toList();
                    case "--threads" -> a.threads = Arrays.stream(v.split(",")).mapToInt(Integer::parseInt).toArray();
                    case "--keys" -> a.keys = Arrays.stream(v.split(",")).mapToInt(Integer::parseInt).toArray();
                    case "--seconds" -> a.seconds = Integer.parseInt(v);
                    case "--warmup" -> a.warmupSeconds = Integer.parseInt(v);
                    case "--redis" -> {
                        String[] hp = v.split(":");
                        a.redisHost = hp[0];
                        a.redisPort = hp.length > 1 ? Integer.parseInt(hp[1]) : 6379;
                    }
                    default -> throw new IllegalArgumentException("unknown option " + argv[i]);
                }
            }
            return a;
        }
    }
}
