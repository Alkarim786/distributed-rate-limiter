// Command server runs the rate limiter gRPC service.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	rlv1 "github.com/Alkarim786/distributed-rate-limiter/gen/ratelimit/v1"
	"github.com/Alkarim786/distributed-rate-limiter/internal/config"
	"github.com/Alkarim786/distributed-rate-limiter/internal/limiter"
	"github.com/Alkarim786/distributed-rate-limiter/internal/metrics"
	"github.com/Alkarim786/distributed-rate-limiter/internal/server"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("server exited", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}
	rules, err := config.LoadRules(cfg.RulesFile)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var lim limiter.Limiter
	switch cfg.Backend {
	case "memory":
		mem := limiter.NewMemory(nil)
		go mem.RunSweeper(ctx, time.Minute)
		lim = mem
	case "redis":
		rdb := redis.NewClient(&redis.Options{
			Addr:     cfg.RedisAddr,
			Password: cfg.RedisPassword,
			DB:       cfg.RedisDB,
			PoolSize: cfg.RedisPoolSize,
		})
		defer rdb.Close()
		r := limiter.NewRedis(rdb)
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := r.Preload(pctx)
		cancel()
		if err != nil {
			// Not fatal: scripts load lazily, and fail-open covers outages.
			log.Warn("could not preload scripts", "err", err)
		}
		lim = r
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := metrics.New(reg)

	svc := server.New(lim, rules, server.Options{
		Backend: cfg.Backend, FailOpen: cfg.FailOpen, Timeout: cfg.RedisTimeout, Metrics: m, Logger: log,
	})

	gs := grpc.NewServer()
	rlv1.RegisterRateLimiterServer(gs, svc)
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(gs, hs)
	reflection.Register(gs)

	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	hsrv := &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	errc := make(chan error, 2)
	go func() { errc <- gs.Serve(lis) }()
	go func() {
		if err := hsrv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()
	log.Info("rate limiter started", "grpc", cfg.ListenAddr, "metrics", cfg.MetricsAddr,
		"backend", cfg.Backend, "rules", len(rules), "fail_open", cfg.FailOpen)

	select {
	case <-ctx.Done():
		log.Info("shutting down")
	case err := <-errc:
		return err
	}
	hs.Shutdown()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = hsrv.Shutdown(shutdownCtx)
	done := make(chan struct{})
	go func() { gs.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-shutdownCtx.Done():
		gs.Stop()
	}
	return nil
}
