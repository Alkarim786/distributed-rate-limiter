package limiter

import (
	"context"
	"strconv"
	"testing"
	"time"
)

func benchMemory(b *testing.B, alg Algorithm, keys int) {
	m := NewMemory(nil)
	r := Rule{Name: "b", Algorithm: alg, Limit: 1_000_000_000, Window: time.Second}
	names := make([]string, keys)
	for i := range names {
		names[i] = "user-" + strconv.Itoa(i)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.Allow(ctx, r, names[i%keys], 1)
			i++
		}
	})
}

func BenchmarkMemoryTokenBucket(b *testing.B)          { benchMemory(b, TokenBucket, 10_000) }
func BenchmarkMemorySlidingWindowCounter(b *testing.B) { benchMemory(b, SlidingWindowCounter, 10_000) }

// The log stores one entry per admitted request, so benchmark it with a
// realistic limit instead of an effectively infinite one.
func BenchmarkMemorySlidingWindowLog(b *testing.B) {
	m := NewMemory(nil)
	r := Rule{Name: "b", Algorithm: SlidingWindowLog, Limit: 100, Window: time.Second}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.Allow(ctx, r, "user-"+strconv.Itoa(i%10_000), 1)
			i++
		}
	})
}
