# Interview notes: distributed rate limiter

How to present this project in a resume, a "tell me about a project" round and a system design round.

---

## Resume bullets (pick 2)

- Built a **distributed rate limiter** in Java 21 (token bucket, sliding window log and counter) backed by **Redis + atomic Lua scripts**. It held the global limit exactly across 4 nodes × 16 threads (2,089 admitted vs 2,100 allowed), where per-node limiting over-admitted by +297%.
- Designed **lock-free in-process limiters** (CAS on immutable state) at **~100 ns/decision and 14M decisions/s**. Added a **circuit breaker with fail-open / fail-closed / local-fallback modes**, so a Redis outage adds zero latency.
- Shipped it as a **zero-dependency HTTP service** (custom RESP2 client, virtual threads, IETF RateLimit headers, Prometheus metrics) with 47 unit and Redis integration tests, a benchmark harness and GitHub Actions CI.

---

## 2-minute pitch

> "I built a rate limiter that works both in-process and across a fleet. The interesting part is the distributed mode. Each decision is one atomic Lua script in Redis, so N servers share one limit with a single round trip and no race. The script uses Redis' own clock, so skewed app servers can't refill buckets early. Every decision touches exactly one key, so it shards cleanly on Redis Cluster.
>
> I implemented three algorithms because they trade differently. The token bucket allows bursts and costs O(1). The sliding window log is exact but O(limit) memory, which suits logins. The sliding window counter is O(1) and approximate, which suits high-volume quotas.
>
> For failure handling there's a circuit breaker: when Redis is down we stop calling it and either fail open, fail closed, or fall back to a local limiter at limit-divided-by-nodes.
>
> To prove it works I wrote a check with 4 simulated nodes hammering one key: per-node limiting admits 4× the limit, the Redis version admits at most the limit. Locally it's about 100 ns per decision; in Redis mode it's bound by the round trip, so the next optimization would be token leasing or batching."

---

## Deep-dive questions you should expect

**Why Lua instead of MULTI/EXEC or WATCH?**
MULTI/EXEC can't branch on a value it read inside the transaction. WATCH-based optimistic locking needs a retry loop, and under contention on a hot key most attempts abort. A Lua script runs atomically on the Redis thread: read, compute, write, one round trip, no retries.

**Doesn't a Lua script block Redis?**
Yes, all scripts run on Redis' single thread, so they must be short. These are O(1), except the log, which is O(log n + evicted). Measured cost is dominated by the network round trip, not script time.

**Why use Redis TIME instead of passing the client's timestamp?**
App servers' clocks drift. If node A is 2 s ahead, it would refill buckets 2 s early for everyone. One authoritative clock removes that. Trade-off: after a Redis failover the new primary's clock is used. It's NTP-synced, and the algorithms clamp negative elapsed time so it can never mint tokens.

**How does it behave on Redis Cluster?**
Each decision touches one key, `rl:<rule>:<key>`, so there are no cross-slot issues and keys spread across shards. I deliberately avoided a hash tag on the rule name, because it would pin a rule's entire keyspace to one shard.

**What happens when Redis goes down?**
Requests time out fast (100 ms read timeout). After 5 consecutive failures the breaker opens and we stop calling Redis, so there's zero added latency. Then the failure mode applies: open for normal APIs, closed for login/OTP, or local fallback at limit/N. After a 2 s cooldown one probe request tests Redis; success closes the circuit.

**Why is LOCAL_FALLBACK limit/N only approximate?**
Traffic isn't evenly balanced, and N changes with autoscaling. It's a safety net, not a guarantee. Improvement: get N from service discovery.

**Token bucket vs sliding window: when do you pick each?**
Token bucket when bursts are fine and you want a smooth average: general API traffic, and it's what Stripe describes publicly. The exact log when the count must be precise and volume is low: 5 logins per minute. The counter for high volume where ±a few percent is fine and O(1) memory matters.

**What's the error of the sliding window counter?**
It assumes the previous window's requests were evenly spread. If they were all at the end of it, the estimate undercounts. Cloudflare reported a very small error rate for this approach on real traffic. In the worst case it admits somewhat more than the limit near window edges, still far better than a fixed window's 2× boundary burst.

**How did you make the local limiters thread-safe without locks?**
Immutable state record in an AtomicReference. Compute the next state, then compareAndSet; on failure, retry. A test races 16 threads on one key with frozen time and asserts exactly `limit` succeed. Under heavy contention on one key, CAS retries cost throughput (14M → 4M ops/s in the benchmark). That's the argument for sharding by key.

**Why ReentrantLock for the log instead of synchronized?**
In Java 21, `synchronized` pins a virtual thread to its carrier thread while it holds the monitor. The server runs one virtual thread per request, so ReentrantLock avoids pinning.

**Memory growth with millions of keys?**
Redis: every key has a TTL equal to the time after which "no state" equals the current state, so idle keys disappear. Local: a janitor evicts entries that are already equivalent to empty.

**Why write your own Redis client?**
I needed five commands, and owning the protocol layer made timeouts, broken-connection handling and EVALSHA→EVAL fallback explicit and testable. In a company codebase I'd use Lettuce or Jedis behind the same interface.

**How would you scale to 1M decisions/s?**
Redis Cluster shards keys, and throughput scales roughly linearly with shards. Reduce round trips with token leasing: each node grabs tokens in batches, so Redis sees 1/N of the traffic, at the cost of strictness. Add a local negative cache for keys known to be exhausted until their retryAfter. Pipeline when the caller can batch.

**Is it exactly-once? What about Redis replica lag?**
Replication is async, so a failover can lose the last few writes and let a little extra traffic through for one window. For rate limiting that's acceptable; it's a protective mechanism, not a ledger. Paying for consensus (Raft, Redlock) on every request would be the wrong trade.

---

## If asked to "design a rate limiter" on the whiteboard

1. **Clarify:** per user / IP / API key? Hard or soft limit? Global or per region? Latency budget (< 1 ms added)? What happens when the limiter fails?
2. **API:** `tryAcquire(key, permits) -> {allowed, remaining, retryAfter}`; HTTP 429 with `Retry-After` and `RateLimit-*` headers.
3. **Algorithm:** compare token bucket, fixed window (boundary burst), sliding log (memory) and sliding counter. Pick per use case.
4. **Single node:** in-memory map of key to state, CAS updates, TTL eviction.
5. **Many nodes:** the naive per-node approach gives N× over-admission (show the numbers from this project). Centralize state in Redis, make each decision an atomic script, use server time.
6. **Scale:** shard by key on Redis Cluster; hot-key splitting; token leasing to cut round trips.
7. **Reliability:** tight timeouts, circuit breaker, explicit fail-open/closed policy per rule, metrics on degraded decisions.
8. **Where it runs:** API gateway / sidecar / library. This repo supports library and service modes.
