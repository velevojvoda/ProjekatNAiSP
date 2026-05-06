package ratelimit

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"
)

// TokenBucket implementira klasični Token Bucket algoritam za ograničenje
// stope pristupa (rate limiting). Kanta ima maksimalan broj tokena
// (capacity), i svake refillInterval se popuni do vrha.
//
// Stanje se serijalizuje binarno (Marshal/Unmarshal) i čuva u sistemu kao
// običan zapis pod rezervisanim ključem.
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

// Allow proverava da li ima dostupnih tokena. Ako ima, potroši 1 i vrati true.
// Ako nema, vrati false (zahtev odbiti). Pre provere se popuni kanta na osnovu
// proteklog vremena od poslednjeg refill-a.
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

// refill puni kantu na osnovu proteklog vremena. Ako je prošlo N intervala,
// dodaje N * capacity tokena (ali ne preko capacity).
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

// Marshal serijalizuje stanje kante u 32 bajta (binarno):
//
//	[Capacity:8B][Tokens:8B][RefillIntervalNs:8B][LastRefillUnixNano:8B]
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

// Unmarshal restauriše SAMO runtime stanje (broj tokena + vreme poslednjeg
// refill-a) iz snimljenih bajtova. Capacity i refillInterval ostaju onakvi
// kakvi su iz config-a — znači izmena config-a uvek pobeđuje.
func (tb *TokenBucket) Unmarshal(data []byte) error {
	if len(data) < 32 {
		return fmt.Errorf("token bucket: invalid data length %d (expected >=32)", len(data))
	}

	tb.mu.Lock()
	defer tb.mu.Unlock()

	// data[0:8]   = capacity   — IGNORIŠEMO, ostaje iz config-a
	// data[16:24] = interval   — IGNORIŠEMO, ostaje iz config-a
	tb.tokens = int64(binary.LittleEndian.Uint64(data[8:16]))
	tb.lastRefill = time.Unix(0, int64(binary.LittleEndian.Uint64(data[24:32])))

	// Ako je config smanjen ispod broja sačuvanih tokena, klampuj.
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}
	return nil
}

// Snapshot vraća trenutno stanje kao tekstualni opis (za debug/UI).
func (tb *TokenBucket) Snapshot() string {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return fmt.Sprintf("tokens=%d/%d refill=%s lastRefill=%s",
		tb.tokens, tb.capacity, tb.refillInterval, tb.lastRefill.Format(time.RFC3339))
}
