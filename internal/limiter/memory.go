package limiter

import (
	"context"
	"hash/fnv"
	"sync"
	"time"
)

const memoryShards = 64

// Memory is a single-process limiter. State is sharded across mutexes so
// unrelated keys don't contend. Use it for tests, local development, or an
// embedded per-node limiter. It is not shared between replicas.
type Memory struct {
	now    func() time.Time
	shards [memoryShards]shard
}

type shard struct {
	mu      sync.Mutex
	buckets map[string]*tokenBucketState
	logs    map[string][]int64
	counter map[string]*slidingCounterState
	expiry  map[string]int64 // key -> ms after which state can be dropped
}

// NewMemory returns an in-memory limiter. clock may be nil (uses time.Now).
func NewMemory(clock func() time.Time) *Memory {
	if clock == nil {
		clock = time.Now
	}
	m := &Memory{now: clock}
	for i := range m.shards {
		m.shards[i] = shard{
			buckets: map[string]*tokenBucketState{},
			logs:    map[string][]int64{},
			counter: map[string]*slidingCounterState{},
			expiry:  map[string]int64{},
		}
	}
	return m
}

func (m *Memory) shardFor(k string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(k))
	return &m.shards[h.Sum32()%memoryShards]
}

// Allow implements Limiter.
func (m *Memory) Allow(_ context.Context, rule Rule, key string, cost int64) (Decision, error) {
	if err := checkCost(rule, cost); err != nil {
		return Decision{}, err
	}
	k := storageKey(rule, key)
	now := m.now().UnixMilli()
	window := rule.Window.Milliseconds()
	s := m.shardFor(k)

	s.mu.Lock()
	defer s.mu.Unlock()

	var r result
	switch rule.Algorithm {
	case TokenBucket:
		st := s.buckets[k]
		if st == nil {
			st = &tokenBucketState{}
			s.buckets[k] = st
		}
		r = tokenBucketStep(st, now, rule.Limit, window, cost)
	case SlidingWindowLog:
		var log []int64
		log, r = slidingLogStep(s.logs[k], now, rule.Limit, window, cost)
		s.logs[k] = log
	case SlidingWindowCounter:
		st := s.counter[k]
		if st == nil {
			st = &slidingCounterState{}
			s.counter[k] = st
		}
		r = slidingCounterStep(st, now, rule.Limit, window, cost)
	default:
		return Decision{}, rule.Validate()
	}
	s.expiry[k] = now + r.resetMs
	return toDecision(rule, r), nil
}

// Sweep drops state for keys whose budget is fully restored, bounding memory
// for long-running processes. It returns the number of keys removed.
func (m *Memory) Sweep() int {
	now := m.now().UnixMilli()
	removed := 0
	for i := range m.shards {
		s := &m.shards[i]
		s.mu.Lock()
		for k, exp := range s.expiry {
			if now >= exp {
				delete(s.buckets, k)
				delete(s.logs, k)
				delete(s.counter, k)
				delete(s.expiry, k)
				removed++
			}
		}
		s.mu.Unlock()
	}
	return removed
}

// RunSweeper calls Sweep every interval until ctx is cancelled.
func (m *Memory) RunSweeper(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.Sweep()
		}
	}
}

// Len reports the number of tracked keys (for tests and metrics).
func (m *Memory) Len() int {
	n := 0
	for i := range m.shards {
		s := &m.shards[i]
		s.mu.Lock()
		n += len(s.expiry)
		s.mu.Unlock()
	}
	return n
}

func storageKey(rule Rule, key string) string {
	// The braces form a Redis Cluster hash tag so a key's state always lives
	// on one slot.
	return "rl:{" + rule.Name + ":" + key + "}"
}

func toDecision(rule Rule, r result) Decision {
	return Decision{
		Allowed:    r.allowed,
		Limit:      rule.Limit,
		Remaining:  r.remaining,
		RetryAfter: ms(r.retryMs),
		ResetAfter: ms(r.resetMs),
	}
}
