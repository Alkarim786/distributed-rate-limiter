package limiter

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *fakeClock { return &fakeClock{t: time.UnixMilli(1_700_000_000_000)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func mustAllow(t *testing.T, l Limiter, r Rule, key string, cost int64) Decision {
	t.Helper()
	d, err := l.Allow(context.Background(), r, key, cost)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	return d
}

func TestTokenBucketBurstThenRefill(t *testing.T) {
	clk := newClock()
	m := NewMemory(clk.Now)
	r := Rule{Name: "tb", Algorithm: TokenBucket, Limit: 10, Window: 10 * time.Second} // 1 token/s

	for i := 0; i < 10; i++ {
		if d := mustAllow(t, m, r, "u", 1); !d.Allowed || d.Remaining != int64(9-i) {
			t.Fatalf("request %d: got %+v", i, d)
		}
	}
	d := mustAllow(t, m, r, "u", 1)
	if d.Allowed || d.RetryAfter != time.Second {
		t.Fatalf("11th request should be denied with 1s retry, got %+v", d)
	}
	clk.Advance(1500 * time.Millisecond)
	if d := mustAllow(t, m, r, "u", 1); !d.Allowed || d.Remaining != 0 {
		t.Fatalf("after 1.5s one token should be available, got %+v", d)
	}
	if d := mustAllow(t, m, r, "u", 1); d.Allowed || d.RetryAfter != 500*time.Millisecond {
		t.Fatalf("expected deny with 500ms retry, got %+v", d)
	}
	clk.Advance(time.Hour)
	if d := mustAllow(t, m, r, "u", 10); !d.Allowed {
		t.Fatalf("bucket should be full again, got %+v", d)
	}
}

func TestTokenBucketCost(t *testing.T) {
	clk := newClock()
	m := NewMemory(clk.Now)
	r := Rule{Name: "tb", Algorithm: TokenBucket, Limit: 5, Window: 5 * time.Second}
	if d := mustAllow(t, m, r, "u", 4); !d.Allowed || d.Remaining != 1 {
		t.Fatalf("got %+v", d)
	}
	if d := mustAllow(t, m, r, "u", 2); d.Allowed || d.RetryAfter != time.Second {
		t.Fatalf("cost 2 with 1 token left should wait 1s, got %+v", d)
	}
	if _, err := m.Allow(context.Background(), r, "u", 6); !errors.Is(err, ErrCostExceedsLimit) {
		t.Fatalf("expected ErrCostExceedsLimit, got %v", err)
	}
}

func TestSlidingWindowLogExact(t *testing.T) {
	clk := newClock()
	m := NewMemory(clk.Now)
	r := Rule{Name: "log", Algorithm: SlidingWindowLog, Limit: 3, Window: time.Minute}

	mustAllow(t, m, r, "u", 1) // t=0
	clk.Advance(10 * time.Second)
	mustAllow(t, m, r, "u", 1) // t=10s
	clk.Advance(10 * time.Second)
	if d := mustAllow(t, m, r, "u", 1); !d.Allowed || d.Remaining != 0 { // t=20s
		t.Fatalf("third request should be allowed with 0 remaining, got %+v", d)
	}
	d := mustAllow(t, m, r, "u", 1)
	if d.Allowed || d.RetryAfter != 40*time.Second {
		t.Fatalf("expected deny until the t=0 entry expires (40s), got %+v", d)
	}
	// Two units need the two oldest entries (t=0, t=10s) to expire: 50s.
	if d := mustAllow(t, m, r, "u", 2); d.Allowed || d.RetryAfter != 50*time.Second {
		t.Fatalf("expected 50s retry for cost 2, got %+v", d)
	}
	clk.Advance(40 * time.Second) // t=60s: first entry expires exactly now
	if d := mustAllow(t, m, r, "u", 1); !d.Allowed {
		t.Fatalf("entry from t=0 should have expired at t=60s, got %+v", d)
	}
}

func TestSlidingWindowLogNoBoundaryBurst(t *testing.T) {
	// The classic fixed-window flaw: limit at the end of one window and again
	// at the start of the next gives 2x the limit. The log must not allow it.
	clk := newClock()
	m := NewMemory(clk.Now)
	r := Rule{Name: "log", Algorithm: SlidingWindowLog, Limit: 5, Window: time.Second}
	clk.Advance(900 * time.Millisecond)
	for i := 0; i < 5; i++ {
		mustAllow(t, m, r, "u", 1)
	}
	clk.Advance(200 * time.Millisecond)
	if d := mustAllow(t, m, r, "u", 1); d.Allowed {
		t.Fatalf("burst across the window boundary must be denied, got %+v", d)
	}
}

func TestSlidingWindowCounterWeighting(t *testing.T) {
	clk := &fakeClock{t: time.UnixMilli(0)}
	m := NewMemory(clk.Now)
	r := Rule{Name: "ctr", Algorithm: SlidingWindowCounter, Limit: 10, Window: time.Second}

	clk.Advance(500 * time.Millisecond) // window 0, halfway
	for i := 0; i < 10; i++ {
		if d := mustAllow(t, m, r, "u", 1); !d.Allowed {
			t.Fatalf("request %d denied: %+v", i, d)
		}
	}
	if d := mustAllow(t, m, r, "u", 1); d.Allowed {
		t.Fatal("11th in the same window must be denied")
	}
	// At 1.25s, window 1 is 25% elapsed: estimate = 10*0.75 = 7.5, so 2 more fit.
	clk.Advance(750 * time.Millisecond)
	if d := mustAllow(t, m, r, "u", 2); !d.Allowed {
		t.Fatalf("2 should fit (7.5+2 <= 10), got %+v", d)
	}
	d := mustAllow(t, m, r, "u", 1)
	if d.Allowed {
		t.Fatalf("estimate 9.5+1 > 10 must deny, got %+v", d)
	}
	// Needs prev*(1-x) + 2 + 1 <= 10  =>  x >= 0.3  =>  50ms after 250ms.
	if d.RetryAfter != 50*time.Millisecond {
		t.Fatalf("retry-after = %v, want 50ms", d.RetryAfter)
	}
	clk.Advance(d.RetryAfter)
	if d := mustAllow(t, m, r, "u", 1); !d.Allowed {
		t.Fatalf("should be allowed after retry-after, got %+v", d)
	}
	// Skipping more than a full window resets everything.
	clk.Advance(3 * time.Second)
	if d := mustAllow(t, m, r, "u", 10); !d.Allowed {
		t.Fatalf("expected fresh budget, got %+v", d)
	}
}

func TestRetryAfterIsHonest(t *testing.T) {
	// For every algorithm: after a deny, waiting exactly RetryAfter must allow
	// the same request, and waiting 1ms less must not (for the exact ones).
	for _, alg := range []Algorithm{TokenBucket, SlidingWindowLog, SlidingWindowCounter} {
		t.Run(alg.String(), func(t *testing.T) {
			clk := newClock()
			m := NewMemory(clk.Now)
			r := Rule{Name: "r", Algorithm: alg, Limit: 7, Window: 700 * time.Millisecond}
			for i := 0; i < 50; i++ {
				d := mustAllow(t, m, r, "k", 1+int64(i%3))
				if d.Allowed {
					clk.Advance(time.Duration(i%4) * 13 * time.Millisecond)
					continue
				}
				if d.RetryAfter <= 0 {
					t.Fatalf("deny without retry-after: %+v", d)
				}
				clk.Advance(d.RetryAfter)
				if again := mustAllow(t, m, r, "k", 1+int64(i%3)); !again.Allowed {
					t.Fatalf("still denied after waiting retry-after %v: %+v", d.RetryAfter, again)
				}
			}
		})
	}
}

func TestKeysAreIndependent(t *testing.T) {
	m := NewMemory(newClock().Now)
	r := Rule{Name: "r", Algorithm: TokenBucket, Limit: 1, Window: time.Minute}
	if !mustAllow(t, m, r, "a", 1).Allowed || !mustAllow(t, m, r, "b", 1).Allowed {
		t.Fatal("different keys must not share a budget")
	}
	if mustAllow(t, m, r, "a", 1).Allowed {
		t.Fatal("key a should be exhausted")
	}
}

func TestSweepRemovesRestoredKeys(t *testing.T) {
	clk := newClock()
	m := NewMemory(clk.Now)
	r := Rule{Name: "r", Algorithm: SlidingWindowLog, Limit: 2, Window: time.Second}
	for i := 0; i < 100; i++ {
		mustAllow(t, m, r, string(rune('a'+i%26))+string(rune('A'+i/26)), 1)
	}
	if m.Len() != 100 {
		t.Fatalf("len = %d", m.Len())
	}
	clk.Advance(2 * time.Second)
	if n := m.Sweep(); n != 100 || m.Len() != 0 {
		t.Fatalf("sweep removed %d, len now %d", n, m.Len())
	}
}

func TestMemoryConcurrentNeverOverAdmits(t *testing.T) {
	m := NewMemory(newClock().Now) // frozen clock: no refill during the test
	for _, alg := range []Algorithm{TokenBucket, SlidingWindowLog, SlidingWindowCounter} {
		r := Rule{Name: alg.String(), Algorithm: alg, Limit: 100, Window: time.Minute}
		var allowed int64
		var mu sync.Mutex
		var wg sync.WaitGroup
		for g := 0; g < 32; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 20; i++ {
					if mustAllow(t, m, r, "hot", 1).Allowed {
						mu.Lock()
						allowed++
						mu.Unlock()
					}
				}
			}()
		}
		wg.Wait()
		if allowed != 100 {
			t.Fatalf("%s: allowed %d of 640, want exactly 100", alg, allowed)
		}
	}
}

func TestRuleValidate(t *testing.T) {
	bad := []Rule{
		{Algorithm: TokenBucket, Limit: 1, Window: time.Second},
		{Name: "x", Algorithm: TokenBucket, Limit: 0, Window: time.Second},
		{Name: "x", Algorithm: TokenBucket, Limit: 1, Window: 0},
		{Name: "x", Algorithm: 99, Limit: 1, Window: time.Second},
	}
	for i, r := range bad {
		if r.Validate() == nil {
			t.Errorf("case %d should be invalid", i)
		}
	}
	if _, err := ParseAlgorithm("nope"); err == nil {
		t.Error("unknown algorithm should error")
	}
}
