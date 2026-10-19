// Package limiter implements rate-limiting algorithms with interchangeable
// in-memory (single node) and Redis (distributed) backends.
package limiter

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Algorithm selects how a rule's budget is tracked.
type Algorithm int

const (
	// TokenBucket refills Limit tokens evenly over Window and allows bursts
	// of up to Limit.
	TokenBucket Algorithm = iota + 1
	// SlidingWindowLog keeps a timestamp per admitted unit; exact, but
	// memory grows with Limit.
	SlidingWindowLog
	// SlidingWindowCounter blends the previous and current fixed windows;
	// O(1) memory with a small approximation error.
	SlidingWindowCounter
)

func (a Algorithm) String() string {
	switch a {
	case TokenBucket:
		return "token_bucket"
	case SlidingWindowLog:
		return "sliding_window_log"
	case SlidingWindowCounter:
		return "sliding_window_counter"
	default:
		return fmt.Sprintf("algorithm(%d)", int(a))
	}
}

// ParseAlgorithm accepts the names produced by Algorithm.String.
func ParseAlgorithm(s string) (Algorithm, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "token_bucket", "tokenbucket":
		return TokenBucket, nil
	case "sliding_window_log", "sliding_log":
		return SlidingWindowLog, nil
	case "sliding_window_counter", "sliding_counter":
		return SlidingWindowCounter, nil
	}
	return 0, fmt.Errorf("unknown algorithm %q", s)
}

// Rule is a named limit: at most Limit units per Window.
type Rule struct {
	Name      string
	Algorithm Algorithm
	Limit     int64
	Window    time.Duration
}

// Validate reports configuration errors.
func (r Rule) Validate() error {
	switch {
	case r.Name == "":
		return errors.New("rule name is required")
	case r.Limit <= 0:
		return fmt.Errorf("rule %q: limit must be > 0", r.Name)
	case r.Window < time.Millisecond:
		return fmt.Errorf("rule %q: window must be >= 1ms", r.Name)
	case r.Algorithm < TokenBucket || r.Algorithm > SlidingWindowCounter:
		return fmt.Errorf("rule %q: invalid algorithm", r.Name)
	}
	return nil
}

// Decision is the outcome of one Allow call.
type Decision struct {
	Allowed   bool
	Limit     int64
	Remaining int64
	// RetryAfter is how long until a request of the same cost could be
	// admitted. Zero when Allowed.
	RetryAfter time.Duration
	// ResetAfter is how long until the budget is fully restored.
	ResetAfter time.Duration
}

// ErrCostExceedsLimit is returned when a single request asks for more units
// than the rule could ever grant.
var ErrCostExceedsLimit = errors.New("cost exceeds rule limit")

// Limiter decides whether key may consume cost units under rule.
type Limiter interface {
	Allow(ctx context.Context, rule Rule, key string, cost int64) (Decision, error)
}

func checkCost(rule Rule, cost int64) error {
	if cost <= 0 {
		return fmt.Errorf("cost must be > 0, got %d", cost)
	}
	if cost > rule.Limit {
		return ErrCostExceedsLimit
	}
	return nil
}

func ms(v int64) time.Duration {
	if v < 0 {
		v = 0
	}
	return time.Duration(v) * time.Millisecond
}
