// Package server implements the RateLimiter gRPC service.
package server

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	rlv1 "github.com/Alkarim786/distributed-rate-limiter/gen/ratelimit/v1"
	"github.com/Alkarim786/distributed-rate-limiter/internal/limiter"
	"github.com/Alkarim786/distributed-rate-limiter/internal/metrics"
)

const maxKeyLen = 256

// Service implements rlv1.RateLimiterServer on top of a limiter.Limiter.
type Service struct {
	rlv1.UnimplementedRateLimiterServer

	limiter  limiter.Limiter
	backend  string
	rules    map[string]limiter.Rule
	failOpen bool
	timeout  time.Duration
	metrics  *metrics.Metrics
	log      *slog.Logger
}

// Options configures a Service.
type Options struct {
	Backend  string // label for metrics, e.g. "redis"
	FailOpen bool   // allow requests when the backend errors
	Timeout  time.Duration
	Metrics  *metrics.Metrics
	Logger   *slog.Logger
}

// New builds a Service.
func New(l limiter.Limiter, rules map[string]limiter.Rule, o Options) *Service {
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return &Service{
		limiter: l, backend: o.Backend, rules: rules, failOpen: o.FailOpen,
		timeout: o.Timeout, metrics: o.Metrics, log: o.Logger,
	}
}

// Check implements rlv1.RateLimiterServer.
func (s *Service) Check(ctx context.Context, req *rlv1.CheckRequest) (*rlv1.CheckResponse, error) {
	rule, ok := s.rules[req.GetRule()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown rule %q", req.GetRule())
	}
	key := req.GetKey()
	if key == "" || len(key) > maxKeyLen {
		return nil, status.Errorf(codes.InvalidArgument, "key must be 1-%d bytes", maxKeyLen)
	}
	cost := req.GetCost()
	if cost == 0 {
		cost = 1
	}
	if cost < 0 {
		return nil, status.Error(codes.InvalidArgument, "cost must be >= 0")
	}
	if cost > rule.Limit {
		return nil, status.Errorf(codes.InvalidArgument, "cost %d exceeds rule limit %d", cost, rule.Limit)
	}

	if s.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}
	start := time.Now()
	d, err := s.limiter.Allow(ctx, rule, key, cost)
	if s.metrics != nil {
		s.metrics.Latency.WithLabelValues(s.backend).Observe(time.Since(start).Seconds())
	}
	if err != nil {
		return s.backendFailure(rule, err)
	}

	result := "denied"
	if d.Allowed {
		result = "allowed"
	}
	s.count(rule.Name, result)
	return &rlv1.CheckResponse{
		Allowed:      d.Allowed,
		Limit:        d.Limit,
		Remaining:    d.Remaining,
		RetryAfterMs: d.RetryAfter.Milliseconds(),
		ResetAfterMs: d.ResetAfter.Milliseconds(),
		Algorithm:    toProtoAlg(rule.Algorithm),
	}, nil
}

func (s *Service) backendFailure(rule limiter.Rule, err error) (*rlv1.CheckResponse, error) {
	if errors.Is(err, limiter.ErrCostExceedsLimit) {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if s.metrics != nil {
		s.metrics.BackendErrors.WithLabelValues(rule.Name).Inc()
	}
	s.log.Warn("rate limit backend error", "rule", rule.Name, "err", err, "fail_open", s.failOpen)
	if !s.failOpen {
		return nil, status.Error(codes.Unavailable, "rate limit backend unavailable")
	}
	s.count(rule.Name, "fail_open")
	return &rlv1.CheckResponse{
		Allowed:   true,
		Limit:     rule.Limit,
		Algorithm: toProtoAlg(rule.Algorithm),
	}, nil
}

func (s *Service) count(rule, result string) {
	if s.metrics != nil {
		s.metrics.Decisions.WithLabelValues(rule, result).Inc()
	}
}

// ListRules implements rlv1.RateLimiterServer.
func (s *Service) ListRules(context.Context, *rlv1.ListRulesRequest) (*rlv1.ListRulesResponse, error) {
	out := make([]*rlv1.Rule, 0, len(s.rules))
	for _, r := range s.rules {
		out = append(out, &rlv1.Rule{
			Name:      r.Name,
			Algorithm: toProtoAlg(r.Algorithm),
			Limit:     r.Limit,
			WindowMs:  r.Window.Milliseconds(),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return &rlv1.ListRulesResponse{Rules: out}, nil
}

func toProtoAlg(a limiter.Algorithm) rlv1.Algorithm {
	switch a {
	case limiter.TokenBucket:
		return rlv1.Algorithm_ALGORITHM_TOKEN_BUCKET
	case limiter.SlidingWindowLog:
		return rlv1.Algorithm_ALGORITHM_SLIDING_WINDOW_LOG
	case limiter.SlidingWindowCounter:
		return rlv1.Algorithm_ALGORITHM_SLIDING_WINDOW_COUNTER
	}
	return rlv1.Algorithm_ALGORITHM_UNSPECIFIED
}
