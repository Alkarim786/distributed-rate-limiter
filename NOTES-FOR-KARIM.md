# Notes for Karim: Distributed Rate Limiter

## Resume bullets (real measured numbers)

- Built a **distributed gRPC rate-limiting service in Go** with token bucket, sliding window log and sliding window counter algorithms, each run as an **atomic Redis Lua script**. Proved exact enforcement across replicas: **31,075 concurrent requests across 2 replicas, exactly 25 admitted** against a limit of 25.
- Benchmarked the full gRPC → limiter → Redis path at **9.1k req/s with p99 17.8 ms** (64 concurrent clients) and **1.7 ms p50** single-client latency on a 4-core laptop. The in-memory sharded backend decides in **85 ns/op** across 8 threads.
- Wrote 25 tests, including a **Redis-vs-Go equivalence test** (6,000 random ops, identical decisions) and **race-detector** CI against a real Redis service container. Shipped with Prometheus metrics, fail-open/closed policy, Docker Compose and GitHub Actions.

## 5 likely interview questions

1. **"Why not just keep a counter in each API server?"**
   With N servers, each one's local counter lets a client through N times the limit, and a load balancer spreads requests unevenly. You need shared state (Redis) and atomic updates on it.

2. **"How do you prevent race conditions between replicas?"**
   The whole read → refill → check → write runs in **one Lua script**. Redis is single-threaded and runs a script to completion before anything else, so two replicas can never interleave. Contrast with WATCH/MULTI: optimistic, so it retries under contention, which is exactly when a limiter is busiest. Point to `TestDistributedReplicasNeverOverAdmit`.

3. **"Token bucket vs sliding window: when do you use which?"**
   - Token bucket: smooth rate with an allowed burst; 2 numbers of state. Default for APIs.
   - Sliding log: exact, and no burst at window boundaries; stores one timestamp per admitted request, so only for small limits (login attempts).
   - Sliding counter: weighted blend of the previous and current fixed windows; O(1) memory and slightly approximate. Good for high-volume per-IP limits.
   - Fixed window (not implemented on purpose) allows 2× the limit across a boundary. `TestSlidingWindowLogNoBoundaryBurst` shows the log doesn't.

4. **"What happens if Redis goes down?"**
   There's a per-call timeout (`REDIS_TIMEOUT`, default 50 ms). Then the policy applies: fail-open (allow, increment `ratelimit_backend_errors_total`, alert on it) or fail-closed (`UNAVAILABLE`). Fail-open is the default because a limiter outage shouldn't become a full outage; use fail-closed for auth/OTP. Improvements: Redis Sentinel/Cluster, or a local in-memory fallback limiter with limit/N per replica.

5. **"How would you scale this to millions of keys / very high QPS?"**
   - Keys are hash-tagged `rl:{rule:key}` so they shard cleanly on Redis Cluster.
   - TTLs keep memory bounded.
   - Hot keys: **local token leasing** (each replica takes a batch of tokens and spends it locally; trades a little accuracy for far fewer round-trips).
   - Pipelining/batching of checks, and running replicas near Redis.
   - My benchmark shows the second replica didn't help on one laptop because everything shared 4 cores. Be honest that scaling needs separate machines.

## Understand this before interviews

### 1. Token bucket math (`algorithms.go` → `tokenBucketStep`)
- `rate = limit / window` tokens per millisecond.
- On each call: `tokens = min(capacity, tokens + elapsed × rate)`, i.e. lazy refill. No background timers; state only updates when someone asks.
- Allowed if `tokens ≥ cost`.
- Denied → `retry_after = ceil((cost − tokens) / rate)`.
- `reset_after = ceil((capacity − tokens) / rate)`, which is also the TTL, because a full bucket equals "no state".

### 2. Sliding log retry-after (`slidingLogStep`)
The log is sorted. To fit `cost` more units you need `count + cost − limit` entries to expire. The one that expires last among them is at index `count + cost − limit − 1`, so `retry_after = log[idx] + window − now`. An entry at time s stops counting when `now ≥ s + window`. That's why the prune condition is `≤ now − window`. Get the boundary right (`<` vs `≤`) and be able to explain it.

### 3. Sliding window counter estimate (`slidingCounterStep`)
`estimate = prev × (window − elapsed)/window + curr`. Example: 10 requests in the last window, 25% into the new window → 7.5 still "count". The retry formula solves `prev × (1 − x/W) + curr + cost ≤ limit` for x. If the current window alone is already too full, it solves in the next window, where curr becomes prev.

### 4. Why the Lua scripts read `TIME`
If each replica sent its own `time.Now()`, a replica whose clock runs 1 s fast would refill tokens early. Using Redis's clock gives every replica the same time source. Since Redis 5, scripts replicate their *effects*, so calling non-deterministic `TIME` is allowed.

### 5. The floating-point bug the tests caught
`1000 × (1 − 7/10)` = `300.00000000000006` → `ceil` = 301. The fix is `ceil(x − 1e-9)` in both Go and Lua. Good story for "tell me about a bug you found": a unit test with exact expectations caught it, and the equivalence test ensured the fix landed in both implementations.

### 6. Fail-open path (`server.go` → `backendFailure`)
`ErrCostExceedsLimit` is a client error, not a backend error, so it's handled first. Everything else counts as a backend error, then fail-open/closed applies.

### 7. Load tester detail worth mentioning
The first version used a context deadline to end the test, so in-flight calls at the end showed up as `DeadlineExceeded` errors. I switched to a stop flag so measurements aren't polluted. Small, but it shows care about benchmark methodology.
