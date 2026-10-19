// Package config loads server settings from environment variables and rate
// limit rules from a YAML file.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/Alkarim786/distributed-rate-limiter/internal/limiter"
)

// Config holds runtime settings. Every field has an env var; see .env.example.
type Config struct {
	ListenAddr    string        // LISTEN_ADDR
	MetricsAddr   string        // METRICS_ADDR
	Backend       string        // BACKEND: "redis" or "memory"
	RedisAddr     string        // REDIS_ADDR
	RedisPassword string        // REDIS_PASSWORD
	RedisDB       int           // REDIS_DB
	RedisTimeout  time.Duration // REDIS_TIMEOUT
	RedisPoolSize int           // REDIS_POOL_SIZE
	FailOpen      bool          // FAIL_OPEN
	RulesFile     string        // RULES_FILE
}

// FromEnv reads Config, applying defaults for unset variables.
func FromEnv() (Config, error) {
	c := Config{
		ListenAddr:    env("LISTEN_ADDR", ":50051"),
		MetricsAddr:   env("METRICS_ADDR", ":9090"),
		Backend:       env("BACKEND", "redis"),
		RedisAddr:     env("REDIS_ADDR", "localhost:6379"),
		RedisPassword: os.Getenv("REDIS_PASSWORD"),
		RulesFile:     env("RULES_FILE", "config/rules.yaml"),
	}
	var err error
	if c.RedisDB, err = strconv.Atoi(env("REDIS_DB", "0")); err != nil {
		return c, fmt.Errorf("REDIS_DB: %w", err)
	}
	if c.RedisPoolSize, err = strconv.Atoi(env("REDIS_POOL_SIZE", "64")); err != nil {
		return c, fmt.Errorf("REDIS_POOL_SIZE: %w", err)
	}
	if c.RedisTimeout, err = time.ParseDuration(env("REDIS_TIMEOUT", "50ms")); err != nil {
		return c, fmt.Errorf("REDIS_TIMEOUT: %w", err)
	}
	if c.FailOpen, err = strconv.ParseBool(env("FAIL_OPEN", "true")); err != nil {
		return c, fmt.Errorf("FAIL_OPEN: %w", err)
	}
	if c.Backend != "redis" && c.Backend != "memory" {
		return c, fmt.Errorf("BACKEND must be redis or memory, got %q", c.Backend)
	}
	return c, nil
}

func env(k, def string) string {
	if v, ok := os.LookupEnv(k); ok && v != "" {
		return v
	}
	return def
}

type rulesFile struct {
	Rules []struct {
		Name      string `yaml:"name"`
		Algorithm string `yaml:"algorithm"`
		Limit     int64  `yaml:"limit"`
		Window    string `yaml:"window"`
	} `yaml:"rules"`
}

// LoadRules parses a rules YAML file.
func LoadRules(path string) (map[string]limiter.Rule, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseRules(b)
}

// ParseRules parses rules YAML, rejecting duplicates and invalid rules.
func ParseRules(b []byte) (map[string]limiter.Rule, error) {
	var f rulesFile
	if err := yaml.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("parse rules: %w", err)
	}
	if len(f.Rules) == 0 {
		return nil, errors.New("no rules defined")
	}
	out := make(map[string]limiter.Rule, len(f.Rules))
	for _, r := range f.Rules {
		alg, err := limiter.ParseAlgorithm(r.Algorithm)
		if err != nil {
			return nil, fmt.Errorf("rule %q: %w", r.Name, err)
		}
		win, err := time.ParseDuration(r.Window)
		if err != nil {
			return nil, fmt.Errorf("rule %q: window: %w", r.Name, err)
		}
		rule := limiter.Rule{Name: r.Name, Algorithm: alg, Limit: r.Limit, Window: win}
		if err := rule.Validate(); err != nil {
			return nil, err
		}
		if _, dup := out[r.Name]; dup {
			return nil, fmt.Errorf("duplicate rule %q", r.Name)
		}
		out[r.Name] = rule
	}
	return out, nil
}
