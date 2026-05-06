package engine

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"ProjekatNAiSP/app/block"
	"ProjekatNAiSP/app/cache"
	"ProjekatNAiSP/app/config"
	"ProjekatNAiSP/app/memtable"
	"ProjekatNAiSP/app/model"
	"ProjekatNAiSP/app/ratelimit"
	"ProjekatNAiSP/app/sstable"
	"ProjekatNAiSP/app/wal"
)

// SystemKeyPrefix identifikuje interne (rezervisane) ključeve koje korisnik ne
// sme da vidi niti da menja preko običnog PUT/GET/DELETE-a.
const SystemKeyPrefix = "__sys_"

// Rezervisani ključ pod kojim se čuva stanje Token Bucket-a (2.2).
const TokenBucketKey = SystemKeyPrefix + "token_bucket__"

// Greške koje engine vraća korisniku.
var (
	ErrReservedKey       = errors.New("reserved key — not accessible to user")
	ErrRateLimitExceeded = errors.New("rate limit exceeded")
)

type FlushFunc func(records []model.Record) error

type Engine struct {
	cfg            *config.Config
	memtables      []memtable.Memtable
	cache          *cache.LRUCache
	wal            *wal.WAL
	flushFn        FlushFunc
	blockManager   *block.BlockManager
	sstableManager *sstable.Manager
	tables         []*sstable.Table
	tokenBucket    *ratelimit.TokenBucket
}

func NewEngine(cfg *config.Config) (*Engine, error) {
	w, err := wal.NewWAL(cfg.WALDir, cfg.BlockSizeKB*1024, cfg.WALSegmentBlocks)
	if err != nil {
		return nil, err
	}

	activeMem := createMemtable(cfg)
	bm, err := block.NewBlockManager(cfg.BlockSizeKB, cfg.CacheCapacity)
	if err != nil {
		return nil, err
	}

	mgr := sstable.NewManager(filepath.Join(cfg.DataDir, "sstable"), sstable.BuildOptions{
		BlockSize:   cfg.BlockSizeKB * 1024,
		SummaryStep: cfg.SummaryStep,
	})

	tables, err := mgr.LoadExistingTables()
	if err != nil {
		return nil, err
	}

	tb := ratelimit.NewTokenBucket(
		cfg.RateLimitTokens,
		time.Duration(cfg.RateLimitIntervalMs)*time.Millisecond,
	)

	e := &Engine{
		cfg:            cfg,
		memtables:      []memtable.Memtable{activeMem},
		cache:          cache.NewLRUCache(cfg.CacheCapacity),
		wal:            w,
		flushFn:        nil,
		blockManager:   bm,
		sstableManager: mgr,
		tables:         tables,
		tokenBucket:    tb,
	}

	e.flushFn = func(records []model.Record) error {
		table, err := e.sstableManager.BuildFromRecords(records)
		if err != nil {
			return err
		}
		e.tables = append(e.tables, table)
		return nil
	}

	return e, nil
}

func (e *Engine) Recover() error {
	records, err := e.wal.ReadAllRecords()
	if err != nil {
		return err
	}

	for _, rec := range records {
		switch rec.Op {
		case wal.OpPut:
			if err := e.applyPut(rec.Key, rec.Value); err != nil {
				return err
			}
			e.cache.Delete(rec.Key)

		case wal.OpDelete:
			if err := e.applyDelete(rec.Key); err != nil {
				return err
			}
			e.cache.Delete(rec.Key)
		}
	}

	// Pošto je sad sve iz WAL-a primenjeno, učitaj poslednje stanje token
	// bucket-a iz storage-a (ako postoji).
	if data, err := e.internalGet(TokenBucketKey); err == nil && data != nil {
		_ = e.tokenBucket.Unmarshal(data)
	}

	return nil
}

func (e *Engine) SetFlushFunc(fn FlushFunc) {
	if fn != nil {
		e.flushFn = fn
	}
}

// ===== Korisnička JAVNA API — proverava rezervisane ključeve i rate limit =====

func (e *Engine) Put(key string, value []byte) error {
	if isReservedKey(key) {
		return ErrReservedKey
	}
	if !e.tokenBucket.Allow() {
		return ErrRateLimitExceeded
	}
	return e.internalPut(key, value)
}

func (e *Engine) Get(key string) ([]byte, error) {
	if isReservedKey(key) {
		return nil, ErrReservedKey
	}
	if !e.tokenBucket.Allow() {
		return nil, ErrRateLimitExceeded
	}
	return e.internalGet(key)
}

func (e *Engine) Delete(key string) error {
	if isReservedKey(key) {
		return ErrReservedKey
	}
	if !e.tokenBucket.Allow() {
		return ErrRateLimitExceeded
	}
	return e.internalDelete(key)
}

// ===== INTERNI API — bez provera (koristi engine sam za sebe) =====

func (e *Engine) internalPut(key string, value []byte) error {
	if err := e.wal.AppendPut(key, value); err != nil {
		return err
	}
	if err := e.applyPut(key, value); err != nil {
		return err
	}
	e.cache.Put(key, value)
	return nil
}

func (e *Engine) internalGet(key string) ([]byte, error) {
	if value, ok := e.cache.Get(key); ok {
		return value, nil
	}

	for i := len(e.memtables) - 1; i >= 0; i-- {
		record, ok := e.memtables[i].Get(key)
		if !ok {
			continue
		}
		if record.Tombstone {
			e.cache.Delete(key)
			return nil, nil
		}
		e.cache.Put(key, record.Value)
		return record.Value, nil
	}

	for i := len(e.tables) - 1; i >= 0; i-- {
		res, err := e.sstableManager.Get(e.tables[i], key)
		if err != nil {
			return nil, err
		}
		if !res.Found {
			continue
		}
		if res.Record.Tombstone {
			e.cache.Delete(key)
			return nil, nil
		}
		e.cache.Put(key, res.Record.Value)
		return append([]byte(nil), res.Record.Value...), nil
	}

	return nil, nil
}

func (e *Engine) internalDelete(key string) error {
	if err := e.wal.AppendDelete(key); err != nil {
		return err
	}
	if err := e.applyDelete(key); err != nil {
		return err
	}
	e.cache.Delete(key)
	return nil
}

// ===== ostalo =====

func (e *Engine) ReadBlock(path string, blockNumber int64) ([]byte, error) {
	return e.blockManager.ReadBlock(path, blockNumber)
}

func (e *Engine) WriteBlock(path string, blockNumber int64, data []byte) error {
	return e.blockManager.WriteBlock(path, blockNumber, data)
}

func (e *Engine) BlockManager() *block.BlockManager {
	return e.blockManager
}

func (e *Engine) Shutdown() {
	// Snimi trenutno stanje token bucket-a kao običan zapis u sistem.
	_ = e.internalPut(TokenBucketKey, e.tokenBucket.Marshal())
	_ = e.wal.Close()
}

// ListTables vraća ID-eve svih SSTable koje engine drži učitane.
func (e *Engine) ListTables() []string {
	out := make([]string, 0, len(e.tables))
	for _, t := range e.tables {
		out = append(out, t.ID)
	}
	return out
}

// ValidateTable pokreće Merkle validaciju (1.3.5).
func (e *Engine) ValidateTable(id string) (sstable.ValidationResult, error) {
	for _, t := range e.tables {
		if t.ID == id {
			return e.sstableManager.Validate(t)
		}
	}
	return sstable.ValidationResult{}, fmt.Errorf("table %s not found", id)
}

// ===== helperi =====

// isReservedKey vraća true ako ključ pripada internom prostoru (rezervisani).
func isReservedKey(key string) bool {
	return strings.HasPrefix(key, SystemKeyPrefix)
}

func (e *Engine) applyPut(key string, value []byte) error {
	active := e.activeMemtable()

	if active.IsFull() {
		if err := e.rotateMemtableIfNeeded(); err != nil {
			return err
		}
		active = e.activeMemtable()
	}

	return active.Put(key, value)
}

func (e *Engine) applyDelete(key string) error {
	active := e.activeMemtable()

	if active.IsFull() {
		if err := e.rotateMemtableIfNeeded(); err != nil {
			return err
		}
		active = e.activeMemtable()
	}

	return active.Delete(key)
}

func (e *Engine) activeMemtable() memtable.Memtable {
	return e.memtables[len(e.memtables)-1]
}

func (e *Engine) rotateMemtableIfNeeded() error {
	if len(e.memtables) < e.cfg.MemtableCount {
		e.memtables = append(e.memtables, createMemtable(e.cfg))
		return nil
	}

	return e.flushIfNeeded()
}

func (e *Engine) flushIfNeeded() error {
	if !e.allMemtablesFull() {
		return nil
	}

	records := e.collectFlushRecords()

	if err := e.flushFn(records); err != nil {
		return err
	}

	e.resetMemtables()
	return nil
}

func (e *Engine) allMemtablesFull() bool {
	if len(e.memtables) < e.cfg.MemtableCount {
		return false
	}
	for _, mt := range e.memtables {
		if !mt.IsFull() {
			return false
		}
	}
	return true
}

func (e *Engine) collectFlushRecords() []model.Record {
	latest := make(map[string]model.Record)
	for i := len(e.memtables) - 1; i >= 0; i-- {
		records := e.memtables[i].Records()
		for _, record := range records {
			if _, exists := latest[record.Key]; !exists {
				latest[record.Key] = record
			}
		}
	}
	result := make([]model.Record, 0, len(latest))
	for _, record := range latest {
		result = append(result, record)
	}
	return result
}

func (e *Engine) resetMemtables() {
	e.memtables = []memtable.Memtable{createMemtable(e.cfg)}
}

func createMemtable(cfg *config.Config) memtable.Memtable {
	switch cfg.MemtableImpl {
	case "hashmap":
		return memtable.NewHashMapMemtable(cfg.MemtableMaxEntries, cfg.MemtableMaxSizeKB, cfg.MemtableSizeType)
	case "skiplist":
		return memtable.NewSkipListMemtable(cfg.MemtableMaxEntries, cfg.MemtableMaxSizeKB, cfg.MemtableSizeType)
	case "btree":
		return memtable.NewBTreeMemtable(cfg.MemtableMaxEntries, cfg.MemtableMaxSizeKB, cfg.MemtableSizeType)
	default:
		return memtable.NewHashMapMemtable(cfg.MemtableMaxEntries, cfg.MemtableMaxSizeKB, cfg.MemtableSizeType)
	}
}
