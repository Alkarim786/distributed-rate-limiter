package server

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	rlv1 "github.com/Alkarim786/distributed-rate-limiter/gen/ratelimit/v1"
	"github.com/Alkarim786/distributed-rate-limiter/internal/limiter"
	"github.com/Alkarim786/distributed-rate-limiter/internal/metrics"
)

var testRules = map[string]limiter.Rule{
	"api": {Name: "api", Algorithm: limiter.TokenBucket, Limit: 3, Window: time.Minute},
	"log": {Name: "log", Algorithm: limiter.SlidingWindowLog, Limit: 2, Window: time.Minute},
}

type failing struct{}

func (failing) Allow(context.Context, limiter.Rule, string, int64) (limiter.Decision, error) {
	return limiter.Decision{}, errors.New("redis: connection refused")
}

func startServer(t *testing.T, l limiter.Limiter, failOpen bool) (rlv1.RateLimiterClient, *metrics.Metrics) {
	t.Helper()
	m := metrics.New(prometheus.NewRegistry())
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	rlv1.RegisterRateLimiterServer(gs, New(l, testRules, Options{Backend: "test", FailOpen: failOpen, Metrics: m}))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	cc, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	return rlv1.NewRateLimiterClient(cc), m
}

func TestCheckAllowsThenDenies(t *testing.T) {
	c, m := startServer(t, limiter.NewMemory(nil), true)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		resp, err := c.Check(ctx, &rlv1.CheckRequest{Rule: "api", Key: "u1"})
		if err != nil || !resp.Allowed || resp.Limit != 3 {
			t.Fatalf("request %d: %v %+v", i, err, resp)
		}
	}
	resp, err := c.Check(ctx, &rlv1.CheckRequest{Rule: "api", Key: "u1"})
	if err != nil || resp.Allowed || resp.RetryAfterMs <= 0 {
		t.Fatalf("4th request should be denied with retry-after: %v %+v", err, resp)
	}
	if resp.Algorithm != rlv1.Algorithm_ALGORITHM_TOKEN_BUCKET {
		t.Fatalf("algorithm = %v", resp.Algorithm)
	}
	if got := testutil.ToFloat64(m.Decisions.WithLabelValues("api", "denied")); got != 1 {
		t.Fatalf("denied metric = %v", got)
	}
}

func TestCheckValidation(t *testing.T) {
	c, _ := startServer(t, limiter.NewMemory(nil), true)
	ctx := context.Background()
	cases := []struct {
		req  *rlv1.CheckRequest
		code codes.Code
	}{
		{&rlv1.CheckRequest{Rule: "missing", Key: "k"}, codes.NotFound},
		{&rlv1.CheckRequest{Rule: "api", Key: ""}, codes.InvalidArgument},
		{&rlv1.CheckRequest{Rule: "api", Key: "k", Cost: -1}, codes.InvalidArgument},
		{&rlv1.CheckRequest{Rule: "api", Key: "k", Cost: 4}, codes.InvalidArgument},
	}
	for _, tc := range cases {
		_, err := c.Check(ctx, tc.req)
		if status.Code(err) != tc.code {
			t.Errorf("%+v: got %v, want %v", tc.req, status.Code(err), tc.code)
		}
	}
}

func TestFailOpenAndFailClosed(t *testing.T) {
	ctx := context.Background()
	open, m := startServer(t, failing{}, true)
	resp, err := open.Check(ctx, &rlv1.CheckRequest{Rule: "api", Key: "k"})
	if err != nil || !resp.Allowed {
		t.Fatalf("fail-open should allow: %v %+v", err, resp)
	}
	if testutil.ToFloat64(m.BackendErrors.WithLabelValues("api")) != 1 ||
		testutil.ToFloat64(m.Decisions.WithLabelValues("api", "fail_open")) != 1 {
		t.Fatal("fail-open must be counted")
	}

	closed, _ := startServer(t, failing{}, false)
	if _, err := closed.Check(ctx, &rlv1.CheckRequest{Rule: "api", Key: "k"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("fail-closed should return Unavailable, got %v", err)
	}
}

func TestListRules(t *testing.T) {
	c, _ := startServer(t, limiter.NewMemory(nil), true)
	resp, err := c.ListRules(context.Background(), &rlv1.ListRulesRequest{})
	if err != nil || len(resp.Rules) != 2 || resp.Rules[0].Name != "api" || resp.Rules[1].WindowMs != 60000 {
		t.Fatalf("ListRules: %v %+v", err, resp)
	}
}
