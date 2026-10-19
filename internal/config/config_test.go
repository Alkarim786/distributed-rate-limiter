package config

import (
	"testing"
	"time"

	"github.com/Alkarim786/distributed-rate-limiter/internal/limiter"
)

func TestParseRules(t *testing.T) {
	rules, err := ParseRules([]byte(`
rules:
  - {name: a, algorithm: token_bucket, limit: 10, window: 1s}
  - {name: b, algorithm: sliding_window_log, limit: 5, window: 15m}
`))
	if err != nil {
		t.Fatal(err)
	}
	if got := rules["b"]; got.Algorithm != limiter.SlidingWindowLog || got.Window != 15*time.Minute || got.Limit != 5 {
		t.Fatalf("rule b = %+v", got)
	}
}

func TestParseRulesErrors(t *testing.T) {
	cases := map[string]string{
		"empty":     `rules: []`,
		"algorithm": `rules: [{name: a, algorithm: leaky, limit: 1, window: 1s}]`,
		"window":    `rules: [{name: a, algorithm: token_bucket, limit: 1, window: soon}]`,
		"limit":     `rules: [{name: a, algorithm: token_bucket, limit: 0, window: 1s}]`,
		"duplicate": `rules: [{name: a, algorithm: token_bucket, limit: 1, window: 1s}, {name: a, algorithm: token_bucket, limit: 1, window: 1s}]`,
	}
	for name, y := range cases {
		if _, err := ParseRules([]byte(y)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestDefaultRulesFileIsValid(t *testing.T) {
	if _, err := LoadRules("../../config/rules.yaml"); err != nil {
		t.Fatal(err)
	}
}

func TestFromEnv(t *testing.T) {
	t.Setenv("BACKEND", "memory")
	t.Setenv("REDIS_TIMEOUT", "20ms")
	t.Setenv("FAIL_OPEN", "false")
	c, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.Backend != "memory" || c.RedisTimeout != 20*time.Millisecond || c.FailOpen {
		t.Fatalf("config = %+v", c)
	}
	t.Setenv("BACKEND", "etcd")
	if _, err := FromEnv(); err == nil {
		t.Fatal("invalid backend should error")
	}
}
