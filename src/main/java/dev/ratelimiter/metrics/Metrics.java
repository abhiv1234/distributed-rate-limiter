package dev.ratelimiter.metrics;

import dev.ratelimiter.api.Decision;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Lock-free counters and a fixed-bucket latency histogram, rendered in Prometheus text format.
 * {@link LongAdder} keeps the hot path contention-free under many threads.
 */
public final class Metrics {

    /** Histogram upper bounds in microseconds. */
    private static final long[] BUCKETS_US = {50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 100_000};

    private final Map<String, RuleMetrics> byRule = new ConcurrentHashMap<>();

    public void record(String rule, Decision d, long latencyMicros) {
        RuleMetrics m = byRule.computeIfAbsent(rule, r -> new RuleMetrics());
        (d.allowed() ? m.allowed : m.denied).increment();
        if (d.degraded()) {
            m.degraded.increment();
        }
        m.latencySumUs.add(latencyMicros);
        m.latencyCount.increment();
        for (int i = 0; i < BUCKETS_US.length; i++) {
            if (latencyMicros <= BUCKETS_US[i]) {
                m.buckets[i].increment();
                return;
            }
        }
        m.buckets[BUCKETS_US.length].increment();
    }

    public long allowed(String rule) {
        RuleMetrics m = byRule.get(rule);
        return m == null ? 0 : m.allowed.sum();
    }

    public long denied(String rule) {
        RuleMetrics m = byRule.get(rule);
        return m == null ? 0 : m.denied.sum();
    }

    /** Appends this registry's series in Prometheus exposition format. */
    public void render(StringBuilder out) {
        out.append("# HELP ratelimiter_decisions_total Rate limit decisions by rule and outcome.\n");
        out.append("# TYPE ratelimiter_decisions_total counter\n");
        byRule.forEach((rule, m) -> {
            line(out, "ratelimiter_decisions_total", "rule=\"" + rule + "\",outcome=\"allowed\"", m.allowed.sum());
            line(out, "ratelimiter_decisions_total", "rule=\"" + rule + "\",outcome=\"denied\"", m.denied.sum());
        });
        out.append("# HELP ratelimiter_degraded_decisions_total Decisions made without Redis (failure mode).\n");
        out.append("# TYPE ratelimiter_degraded_decisions_total counter\n");
        byRule.forEach((rule, m) -> line(out, "ratelimiter_degraded_decisions_total", "rule=\"" + rule + "\"", m.degraded.sum()));

        out.append("# HELP ratelimiter_decision_latency_seconds Time to make a decision.\n");
        out.append("# TYPE ratelimiter_decision_latency_seconds histogram\n");
        byRule.forEach((rule, m) -> {
            long cumulative = 0;
            for (int i = 0; i < BUCKETS_US.length; i++) {
                cumulative += m.buckets[i].sum();
                line(out, "ratelimiter_decision_latency_seconds_bucket",
                        "rule=\"" + rule + "\",le=\"" + (BUCKETS_US[i] / 1_000_000.0) + "\"", cumulative);
            }
            cumulative += m.buckets[BUCKETS_US.length].sum();
            line(out, "ratelimiter_decision_latency_seconds_bucket", "rule=\"" + rule + "\",le=\"+Inf\"", cumulative);
            out.append("ratelimiter_decision_latency_seconds_sum{rule=\"").append(rule).append("\"} ")
                    .append(m.latencySumUs.sum() / 1_000_000.0).append('\n');
            line(out, "ratelimiter_decision_latency_seconds_count", "rule=\"" + rule + "\"", m.latencyCount.sum());
        });
    }

    public static void line(StringBuilder out, String name, String labels, long value) {
        out.append(name);
        if (labels != null && !labels.isEmpty()) {
            out.append('{').append(labels).append('}');
        }
        out.append(' ').append(value).append('\n');
    }

    private static final class RuleMetrics {
        final LongAdder allowed = new LongAdder();
        final LongAdder denied = new LongAdder();
        final LongAdder degraded = new LongAdder();
        final LongAdder latencySumUs = new LongAdder();
        final LongAdder latencyCount = new LongAdder();
        final LongAdder[] buckets = new LongAdder[BUCKETS_US.length + 1];

        RuleMetrics() {
            for (int i = 0; i < buckets.length; i++) {
                buckets[i] = new LongAdder();
            }
        }
    }
}
