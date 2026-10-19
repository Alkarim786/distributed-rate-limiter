package limiter

import "math"

// The functions in this file are the single source of truth for the
// algorithms' arithmetic. The in-memory backend calls them directly and the
// Redis Lua scripts (scripts/*.lua) are line-for-line ports, so both backends
// make identical decisions for the same inputs. All times are integer
// milliseconds.

// tokenBucketState is persisted per key.
type tokenBucketState struct {
	tokens float64
	ts     int64 // last refill time (ms)
	init   bool
}

type result struct {
	allowed   bool
	remaining int64
	retryMs   int64
	resetMs   int64
}

func tokenBucketStep(st *tokenBucketState, now, capacity, windowMs, cost int64) result {
	rate := float64(capacity) / float64(windowMs) // tokens per ms
	if !st.init {
		st.tokens, st.ts, st.init = float64(capacity), now, true
	}
	if now > st.ts {
		st.tokens = math.Min(float64(capacity), st.tokens+float64(now-st.ts)*rate)
		st.ts = now
	}
	var r result
	if st.tokens >= float64(cost) {
		st.tokens -= float64(cost)
		r.allowed = true
	} else {
		r.retryMs = int64(ceilEps((float64(cost) - st.tokens) / rate))
	}
	r.remaining = int64(math.Floor(st.tokens))
	r.resetMs = int64(ceilEps((float64(capacity) - st.tokens) / rate))
	return r
}

// slidingLogStep operates on timestamps sorted ascending. An entry with
// timestamp s counts while now < s+window.
func slidingLogStep(log []int64, now, limit, windowMs, cost int64) ([]int64, result) {
	cut := 0
	for cut < len(log) && log[cut] <= now-windowMs {
		cut++
	}
	if cut > 0 {
		// Copy rather than reslice so the backing array doesn't keep growing
		// at the front for a long-lived hot key.
		log = append(log[:0], log[cut:]...)
	}
	count := int64(len(log))
	var r result
	if count+cost <= limit {
		for i := int64(0); i < cost; i++ {
			log = append(log, now)
		}
		count += cost
		r.allowed = true
	} else {
		// The (count+cost-limit)-th oldest entry has to expire first.
		idx := count + cost - limit - 1
		r.retryMs = log[idx] + windowMs - now
	}
	r.remaining = max64(0, limit-count)
	if len(log) > 0 {
		r.resetMs = log[len(log)-1] + windowMs - now
	}
	return log, r
}

type slidingCounterState struct {
	window int64 // index of the current fixed window
	curr   int64
	prev   int64
	init   bool
}

func slidingCounterStep(st *slidingCounterState, now, limit, windowMs, cost int64) result {
	idx := now / windowMs
	switch {
	case !st.init:
		*st = slidingCounterState{window: idx, init: true}
	case idx == st.window+1:
		st.prev, st.curr, st.window = st.curr, 0, idx
	case idx > st.window+1:
		st.prev, st.curr, st.window = 0, 0, idx
	}
	elapsed := now - idx*windowMs
	weight := float64(windowMs-elapsed) / float64(windowMs)
	est := float64(st.prev)*weight + float64(st.curr)

	var r result
	if est+float64(cost) <= float64(limit) {
		st.curr += cost
		est += float64(cost)
		r.allowed = true
	} else {
		r.retryMs = slidingCounterRetry(st, elapsed, limit, windowMs, cost)
	}
	r.remaining = max64(0, int64(math.Floor(float64(limit)-est)))
	switch {
	case st.curr > 0:
		r.resetMs = (windowMs - elapsed) + windowMs
	case st.prev > 0:
		r.resetMs = windowMs - elapsed
	}
	return r
}

// slidingCounterRetry finds when prev*(1-x/W) + curr + cost <= limit becomes
// true, either later in this window or in the next one (where curr becomes
// prev).
func slidingCounterRetry(st *slidingCounterState, elapsed, limit, windowMs, cost int64) int64 {
	w := float64(windowMs)
	if st.curr+cost <= limit && st.prev > 0 {
		x := w * (1 - float64(limit-st.curr-cost)/float64(st.prev))
		return max64(1, int64(ceilEps(x))-elapsed)
	}
	x := w * (1 - float64(limit-cost)/float64(st.curr))
	return max64(1, (windowMs-elapsed)+int64(ceilEps(math.Max(0, x))))
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// ceilEps rounds up while ignoring floating-point noise such as
// 1000*(1-0.7) = 300.00000000000006, which would otherwise ceil to 301.
func ceilEps(x float64) float64 { return math.Ceil(x - 1e-9) }
