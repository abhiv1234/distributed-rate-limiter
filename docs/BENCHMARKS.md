# Benchmarks

Raw output of:

```bash
java -cp target/classes dev.ratelimiter.bench.Benchmark --mode local,redis --threads 1,8 --keys 1,10000 --seconds 3 --warmup 1
```

**Environment:** 2-vCPU cloud VM, OpenJDK 21, Redis 7.0 on the same host (loopback, so network RTT is near zero; real deployments add a network round trip per decision). Latency is per `tryAcquire` call, sampled on every 16th call. `keys=1` means every thread hits one hot key (worst-case contention); `keys=10000` spreads load.

Treat these as relative numbers. Rerun on your own hardware; on a laptop with more cores the 8-thread local numbers rise considerably.

| mode  | algorithm              | threads | keys   | ops/sec      | p50 (us) | p99 (us) | p99.9 (us) |
|-------|------------------------|---------|--------|--------------|----------|----------|------------|
| local | TOKEN_BUCKET           |       1 |      1 |   11,503,495 |      0.1 |      0.2 |        0.4 |
| local | TOKEN_BUCKET           |       1 |  10000 |    9,187,067 |      0.1 |      0.3 |        0.5 |
| local | TOKEN_BUCKET           |       8 |      1 |    4,230,786 |      0.3 |      7.3 |       17.2 |
| local | TOKEN_BUCKET           |       8 |  10000 |   13,977,888 |      0.1 |      0.5 |        1.1 |
| local | SLIDING_WINDOW_LOG     |       1 |      1 |   14,550,020 |      0.1 |      0.1 |        0.3 |
| local | SLIDING_WINDOW_LOG     |       1 |  10000 |    3,123,916 |      0.4 |      0.8 |        8.5 |
| local | SLIDING_WINDOW_LOG     |       8 |      1 |   12,586,671 |      0.1 |      0.1 |        1.5 |
| local | SLIDING_WINDOW_LOG     |       8 |  10000 |    5,277,697 |      0.4 |      1.0 |       11.2 |
| local | SLIDING_WINDOW_COUNTER |       1 |      1 |    9,242,532 |      0.1 |      0.2 |        1.9 |
| local | SLIDING_WINDOW_COUNTER |       1 |  10000 |    7,978,610 |      0.1 |      0.4 |        1.8 |
| local | SLIDING_WINDOW_COUNTER |       8 |      1 |    4,157,413 |      0.3 |      4.9 |       13.6 |
| local | SLIDING_WINDOW_COUNTER |       8 |  10000 |   13,337,758 |      0.1 |      0.5 |        2.0 |
| redis | TOKEN_BUCKET           |       1 |      1 |       18,451 |     53.9 |    113.7 |      590.7 |
| redis | TOKEN_BUCKET           |       1 |  10000 |       17,450 |     55.8 |    108.7 |      278.4 |
| redis | TOKEN_BUCKET           |       8 |      1 |       52,518 |    144.9 |    294.6 |     1517.0 |
| redis | TOKEN_BUCKET           |       8 |  10000 |       50,535 |    149.2 |    322.9 |     1111.9 |
| redis | SLIDING_WINDOW_LOG     |       1 |      1 |       15,738 |     57.1 |    153.4 |     1456.0 |
| redis | SLIDING_WINDOW_LOG     |       1 |  10000 |       16,007 |     55.8 |    154.6 |      605.2 |
| redis | SLIDING_WINDOW_LOG     |       8 |      1 |       42,026 |    171.9 |    462.1 |     1488.0 |
| redis | SLIDING_WINDOW_LOG     |       8 |  10000 |       41,072 |    181.1 |    470.1 |     1236.8 |
| redis | SLIDING_WINDOW_COUNTER |       1 |      1 |       16,788 |     53.3 |    130.5 |      303.6 |
| redis | SLIDING_WINDOW_COUNTER |       1 |  10000 |       16,889 |     53.7 |    109.0 |      412.1 |
| redis | SLIDING_WINDOW_COUNTER |       8 |      1 |       48,385 |    156.8 |    338.0 |     1502.5 |
| redis | SLIDING_WINDOW_COUNTER |       8 |  10000 |       46,221 |    161.2 |    426.1 |     2683.7 |

## Reading the results

- **Local mode costs ~0.1 µs per decision.** It is effectively free compared to any request it protects.
- **Hot key + CAS (token bucket, counter, 8 threads, 1 key):** throughput drops to roughly 4M ops/s and p99 grows to several µs. That is CAS retry contention on one `AtomicReference`. With 10,000 keys contention disappears and throughput is the highest in the table. This is the classic argument for sharding state by key.
- **Sliding window log is cheap on a hot key** in this run because the key sits at its limit: each call evicts at most a few expired timestamps and denies. With 10,000 keys it is the slowest local algorithm (more memory, more cache misses): the O(limit) memory cost shows up as time.
- **Redis mode is bound by round trips, not by Lua.** One connection does ~16-18k decisions/s (~55 µs per round trip on loopback); 8 connections reach ~40-52k/s on a 2-core box where Redis and the benchmark compete for CPU. The log script is ~15% slower than the others (ZSET writes plus eviction).
- **Algorithm choice barely matters for Redis throughput**; the network does. To go faster: pipelining/batching, a local pre-check that rejects obviously over-limit keys without calling Redis, or leasing tokens in batches to each node (see README "Next steps").
