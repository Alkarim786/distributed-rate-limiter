package limiter

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"fmt"
	"strconv"
	"sync/atomic"

	"github.com/redis/go-redis/v9"
)

var (
	//go:embed scripts/token_bucket.lua
	tokenBucketLua string
	//go:embed scripts/sliding_window_log.lua
	slidingLogLua string
	//go:embed scripts/sliding_window_counter.lua
	slidingCounterLua string
)

// Redis is a distributed limiter: every decision is one atomic Lua script, so
// any number of limiter replicas can share a Redis and still never admit more
// than a rule allows.
type Redis struct {
	client   redis.Scripter
	scripts  map[Algorithm]*redis.Script
	instance string
	seq      atomic.Uint64
}

// NewRedis wraps a go-redis client (a *redis.Client, *redis.ClusterClient or
// *redis.Ring all work).
func NewRedis(client redis.Scripter) *Redis {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return &Redis{
		client: client,
		scripts: map[Algorithm]*redis.Script{
			TokenBucket:          redis.NewScript(tokenBucketLua),
			SlidingWindowLog:     redis.NewScript(slidingLogLua),
			SlidingWindowCounter: redis.NewScript(slidingCounterLua),
		},
		instance: hex.EncodeToString(b),
	}
}

// Preload loads all scripts into Redis's script cache so the first requests
// use EVALSHA directly.
func (r *Redis) Preload(ctx context.Context) error {
	for alg, s := range r.scripts {
		if err := s.Load(ctx, r.client).Err(); err != nil {
			return fmt.Errorf("load %s script: %w", alg, err)
		}
	}
	return nil
}

// Allow implements Limiter.
func (r *Redis) Allow(ctx context.Context, rule Rule, key string, cost int64) (Decision, error) {
	if err := checkCost(rule, cost); err != nil {
		return Decision{}, err
	}
	script, ok := r.scripts[rule.Algorithm]
	if !ok {
		return Decision{}, rule.Validate()
	}
	args := []any{rule.Limit, rule.Window.Milliseconds(), cost}
	if rule.Algorithm == SlidingWindowLog {
		args = append(args, r.instance+strconv.FormatUint(r.seq.Add(1), 36))
	}
	// Run tries EVALSHA and falls back to EVAL on NOSCRIPT.
	raw, err := script.Run(ctx, r.client, []string{storageKey(rule, key)}, args...).Int64Slice()
	if err != nil {
		return Decision{}, fmt.Errorf("redis %s: %w", rule.Algorithm, err)
	}
	if len(raw) != 4 {
		return Decision{}, fmt.Errorf("redis %s: unexpected reply length %d", rule.Algorithm, len(raw))
	}
	return toDecision(rule, result{
		allowed:   raw[0] == 1,
		remaining: raw[1],
		retryMs:   raw[2],
		resetMs:   raw[3],
	}), nil
}
