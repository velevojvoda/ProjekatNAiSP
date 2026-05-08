package ratelimit

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"
)

type TokenBucket struct {
	capacity       int64
	tokens         int64
	refillInterval time.Duration
	lastRefill     time.Time
	mu             sync.Mutex
}

func NewTokenBucket(capacity int64, refillInterval time.Duration) *TokenBucket {
	if capacity <= 0 {
		capacity = 100
	}
	if refillInterval <= 0 {
		refillInterval = time.Second
	}
	return &TokenBucket{
		capacity:       capacity,
		tokens:         capacity,
		refillInterval: refillInterval,
		lastRefill:     time.Now(),
	}
}

func (tb *TokenBucket) Allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()
	if tb.tokens > 0 {
		tb.tokens--
		return true
	}
	return false
}

func (tb *TokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill)
	if elapsed < tb.refillInterval {
		return
	}

	intervals := int64(elapsed / tb.refillInterval)
	tb.tokens += intervals * tb.capacity
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}
	tb.lastRefill = tb.lastRefill.Add(time.Duration(intervals) * tb.refillInterval)
}

// [Capacity:8B][Tokens:8B][RefillIntervalNs:8B][LastRefillUnixNano:8B]
func (tb *TokenBucket) Marshal() []byte {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	buf := make([]byte, 32)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(tb.capacity))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(tb.tokens))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(tb.refillInterval.Nanoseconds()))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(tb.lastRefill.UnixNano()))
	return buf
}

func (tb *TokenBucket) Unmarshal(data []byte) error {
	if len(data) < 32 {
		return fmt.Errorf("token bucket: invalid data length %d (expected >=32)", len(data))
	}

	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.tokens = int64(binary.LittleEndian.Uint64(data[8:16]))
	tb.lastRefill = time.Unix(0, int64(binary.LittleEndian.Uint64(data[24:32])))

	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}
	return nil
}

func (tb *TokenBucket) Snapshot() string {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return fmt.Sprintf("tokens=%d/%d refill=%s lastRefill=%s",
		tb.tokens, tb.capacity, tb.refillInterval, tb.lastRefill.Format(time.RFC3339))
}
