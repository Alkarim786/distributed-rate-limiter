package limiter

import (
	"context"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestRedis returns a client for REDIS_TEST_ADDR when set (real Redis in
// CI) or for an in-process miniredis, which executes the real Lua scripts.
func newTestRedis(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	if addr := os.Getenv("REDIS_TEST_ADDR"); addr != "" {
		c := redis.NewClient(&redis.Options{Addr: addr})
		if err := c.FlushDB(context.Background()).Err(); err != nil {
			t.Fatalf("flush real redis: %v", err)
		}
		t.Cleanup(func() { c.Close() })
		return c, nil
	}
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { c.Close() })
	return c, mr
}

// TestRedisMatchesMemory replays the same random workload against both
// backends with the same clock. The Lua scripts must make exactly the same
// decisions as the Go implementation.
func TestRedisMatchesMemory(t *testing.T) {
	client, mr := newTestRedis(t)
	if mr == nil {
		t.Skip("needs miniredis to control the clock")
	}
	start := time.UnixMilli(1_700_000_000_000)
	now := start
	mr.SetTime(now)
	mem := NewMemory(func() time.Time { return now })
	red := NewRedis(client)
	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))

	for _, alg := range []Algorithm{TokenBucket, SlidingWindowLog, SlidingWindowCounter} {
		r := Rule{Name: alg.String(), Algorithm: alg, Limit: 20, Window: 2 * time.Second}
		for i := 0; i < 2000; i++ {
			key := []string{"a", "b", "c"}[rng.Intn(3)]
			cost := int64(1 + rng.Intn(4))
			dm, err := mem.Allow(ctx, r, key, cost)
			if err != nil {
				t.Fatal(err)
			}
			dr, err := red.Allow(ctx, r, key, cost)
			if err != nil {
				t.Fatal(err)
			}
			if dm != dr {
				t.Fatalf("%s step %d key=%s cost=%d: memory=%+v redis=%+v", alg, i, key, cost, dm, dr)
			}
			now = now.Add(time.Duration(rng.Intn(120)) * time.Millisecond)
			mr.SetTime(now)
		}
	}
}

// TestDistributedReplicasNeverOverAdmit simulates several limiter replicas
// (separate Redis limiter instances and connection pools) hammering one hot
// key at once.
func TestDistributedReplicasNeverOverAdmit(t *testing.T) {
	_, mr := newTestRedis(t)
	addr := os.Getenv("REDIS_TEST_ADDR")
	if mr != nil {
		mr.SetTime(time.UnixMilli(1_700_000_000_000)) // freeze time: no refill
		addr = mr.Addr()
	}
	const replicas, workers, perWorker, limit = 3, 40, 10, 150

	for _, alg := range []Algorithm{TokenBucket, SlidingWindowLog, SlidingWindowCounter} {
		r := Rule{Name: "dist-" + alg.String(), Algorithm: alg, Limit: limit, Window: time.Hour}
		var allowed atomic.Int64
		var wg sync.WaitGroup
		for rep := 0; rep < replicas; rep++ {
			c := redis.NewClient(&redis.Options{Addr: addr, PoolSize: 16})
			defer c.Close()
			lim := NewRedis(c)
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < perWorker; i++ {
						d, err := lim.Allow(context.Background(), r, "hot-key", 1)
						if err != nil {
							t.Error(err)
							return
						}
						if d.Allowed {
							allowed.Add(1)
						}
					}
				}()
			}
		}
		wg.Wait()
		total := replicas * workers * perWorker
		if got := allowed.Load(); got != limit {
			t.Fatalf("%s: %d replicas admitted %d of %d requests, want exactly %d", alg, replicas, got, total, limit)
		}
	}
}

func TestRedisKeysExpire(t *testing.T) {
	client, mr := newTestRedis(t)
	if mr == nil {
		t.Skip("needs miniredis to fast-forward TTLs")
	}
	red := NewRedis(client)
	ctx := context.Background()
	for _, alg := range []Algorithm{TokenBucket, SlidingWindowLog, SlidingWindowCounter} {
		r := Rule{Name: alg.String(), Algorithm: alg, Limit: 5, Window: time.Second}
		if _, err := red.Allow(ctx, r, "ttl", 1); err != nil {
			t.Fatal(err)
		}
		k := storageKey(r, "ttl")
		if ttl := mr.TTL(k); ttl <= 0 || ttl > 4*time.Second {
			t.Fatalf("%s: key TTL = %v, want a bounded positive TTL", alg, ttl)
		}
		mr.FastForward(5 * time.Second)
		if mr.Exists(k) {
			t.Fatalf("%s: key should have expired", alg)
		}
	}
}

func TestRedisPreloadAndError(t *testing.T) {
	client, mr := newTestRedis(t)
	red := NewRedis(client)
	if err := red.Preload(context.Background()); err != nil {
		t.Fatalf("preload: %v", err)
	}
	if mr == nil {
		return
	}
	mr.Close()
	r := Rule{Name: "x", Algorithm: TokenBucket, Limit: 5, Window: time.Second}
	if _, err := red.Allow(context.Background(), r, "k", 1); err == nil {
		t.Fatal("expected an error when Redis is down")
	}
}

type quietLogger struct{}

func (quietLogger) Printf(context.Context, string, ...interface{}) {}

func TestMain(m *testing.M) {
	// TestRedisPreloadAndError dials a stopped server on purpose; keep the
	// pool's reconnect warnings out of the test output.
	redis.SetLogger(quietLogger{})
	os.Exit(m.Run())
}
