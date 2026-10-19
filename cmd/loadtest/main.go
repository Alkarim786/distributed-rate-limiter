// Command loadtest drives a running rate limiter with concurrent gRPC Check
// calls and reports throughput and latency percentiles.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	rlv1 "github.com/Alkarim786/distributed-rate-limiter/gen/ratelimit/v1"
)

type report struct {
	Target      string           `json:"target"`
	Rule        string           `json:"rule"`
	Concurrency int              `json:"concurrency"`
	Conns       int              `json:"connections"`
	Keys        int              `json:"keys"`
	DurationS   float64          `json:"duration_s"`
	Requests    int64            `json:"requests"`
	Allowed     int64            `json:"allowed"`
	Denied      int64            `json:"denied"`
	Errors      int64            `json:"errors"`
	ErrorCodes  map[string]int64 `json:"error_codes,omitempty"`
	RPS         float64          `json:"rps"`
	P50us       float64          `json:"p50_us"`
	P90us       float64          `json:"p90_us"`
	P99us       float64          `json:"p99_us"`
	Maxus       float64          `json:"max_us"`
	GoMaxProcs  int              `json:"gomaxprocs"`
}

func main() {
	addr := flag.String("addr", "localhost:50051", "limiter gRPC address(es), comma separated")
	rule := flag.String("rule", "bench", "rule name")
	conc := flag.Int("c", 64, "concurrent workers")
	conns := flag.Int("conns", 4, "gRPC connections per address")
	keys := flag.Int("keys", 1000, "distinct keys (spread load)")
	dur := flag.Duration("d", 10*time.Second, "test duration")
	warm := flag.Duration("warmup", 2*time.Second, "warm-up before measuring")
	asJSON := flag.Bool("json", false, "print JSON")
	flag.Parse()

	var clients []rlv1.RateLimiterClient
	for _, a := range splitComma(*addr) {
		for i := 0; i < *conns; i++ {
			cc, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				fmt.Fprintln(os.Stderr, "dial:", err)
				os.Exit(1)
			}
			defer cc.Close()
			clients = append(clients, rlv1.NewRateLimiterClient(cc))
		}
	}

	var measuring atomic.Bool
	var allowed, denied, errs, reqs atomic.Int64
	lat := make([][]time.Duration, *conc)
	var codesMu sync.Mutex
	codes := map[string]int64{}
	// A stop flag instead of a context deadline: calls in flight when the
	// test ends complete normally rather than failing with DeadlineExceeded.
	var stopped atomic.Bool
	ctx := context.Background()
	time.AfterFunc(*warm, func() { measuring.Store(true) })
	time.AfterFunc(*warm+*dur, func() { stopped.Store(true) })

	var wg sync.WaitGroup
	var measureStart atomic.Int64
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c := clients[w%len(clients)]
			i := w
			for !stopped.Load() {
				req := &rlv1.CheckRequest{Rule: *rule, Key: "user-" + strconv.Itoa(i%*keys), Cost: 1}
				i += *conc
				t0 := time.Now()
				resp, err := c.Check(ctx, req)
				el := time.Since(t0)
				if !measuring.Load() {
					continue
				}
				measureStart.CompareAndSwap(0, t0.UnixNano())
				reqs.Add(1)
				switch {
				case err != nil:
					errs.Add(1)
					codesMu.Lock()
					codes[status.Code(err).String()]++
					codesMu.Unlock()
				case resp.Allowed:
					allowed.Add(1)
				default:
					denied.Add(1)
				}
				lat[w] = append(lat[w], el)
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(time.Unix(0, measureStart.Load())).Seconds()

	var all []time.Duration
	for _, l := range lat {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	pct := func(p float64) float64 {
		if len(all) == 0 {
			return 0
		}
		return float64(all[int(p*float64(len(all)-1))].Microseconds())
	}
	r := report{
		Target: *addr, Rule: *rule, Concurrency: *conc, Conns: len(clients), Keys: *keys,
		DurationS: elapsed, Requests: reqs.Load(), Allowed: allowed.Load(), Denied: denied.Load(),
		Errors: errs.Load(), ErrorCodes: codes, RPS: float64(reqs.Load()) / elapsed,
		P50us: pct(.50), P90us: pct(.90), P99us: pct(.99), Maxus: pct(1),
		GoMaxProcs: runtime.GOMAXPROCS(0),
	}
	if *asJSON {
		_ = json.NewEncoder(os.Stdout).Encode(r)
		return
	}
	fmt.Printf("target=%s rule=%s c=%d conns=%d keys=%d\n", r.Target, r.Rule, r.Concurrency, r.Conns, r.Keys)
	fmt.Printf("requests=%d in %.1fs  => %.0f req/s\n", r.Requests, r.DurationS, r.RPS)
	fmt.Printf("allowed=%d denied=%d errors=%d\n", r.Allowed, r.Denied, r.Errors)
	fmt.Printf("latency p50=%.0fus p90=%.0fus p99=%.0fus max=%.0fus\n", r.P50us, r.P90us, r.P99us, r.Maxus)
}

func splitComma(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	return out
}
