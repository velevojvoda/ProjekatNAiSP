package engine

import (
	"path/filepath"

	"ProjekatNAiSP/app/block"
	"ProjekatNAiSP/app/cache"
	"ProjekatNAiSP/app/config"
	"ProjekatNAiSP/app/memtable"
	"ProjekatNAiSP/app/model"
	"ProjekatNAiSP/app/sstable"
	"ProjekatNAiSP/app/wal"
)

type FlushFunc func(records []model.Record) error

type Engine struct {
	cfg          *config.Config
	memtables    []memtable.Memtable
	cache        *cache.LRUCache
	wal          *wal.WAL
	flushFn      FlushFunc
	blockManager *block.BlockManager
	sstableManager *sstable.Manager
	tables         []*sstable.Table
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
		SummaryStep: 3,
	})

	tables, err := mgr.LoadExistingTables()
	if err != nil {
		return nil, err
	}

	e := &Engine{
		cfg:       cfg,
		memtables: []memtable.Memtable{activeMem},
		cache:     cache.NewLRUCache(cfg.CacheCapacity),
		wal:       w,
		flushFn: nil,
		blockManager: bm,
		sstableManager: mgr,
		tables:         tables,
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

	return nil
}

func (e *Engine) SetFlushFunc(fn FlushFunc) {
	if fn != nil {
		e.flushFn = fn
	}
}

func (e *Engine) Put(key string, value []byte) error {
	if err := e.wal.AppendPut(key, value); err != nil {
		return err
	}

	if err := e.applyPut(key, value); err != nil {
		return err
	}

	e.cache.Put(key, value)
	return nil
}

func (e *Engine) Get(key string) ([]byte, error) {
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

func (e *Engine) Delete(key string) error {
	if err := e.wal.AppendDelete(key); err != nil {
		return err
	}

	if err := e.applyDelete(key); err != nil {
		return err
	}

	e.cache.Delete(key)
	return nil
}

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
	_ = e.wal.Close()
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
	e.memtables = []memtable.Memtable{
		createMemtable(e.cfg),
	}
}

func createMemtable(cfg *config.Config) memtable.Memtable {
	switch cfg.MemtableImpl {
	case "hashmap":
		return memtable.NewHashMapMemtable(
			cfg.MemtableMaxEntries,
			cfg.MemtableMaxSizeKB,
			cfg.MemtableSizeType,
		)
	case "skiplist":
		return memtable.NewSkipListMemtable(
			cfg.MemtableMaxEntries,
			cfg.MemtableMaxSizeKB,
			cfg.MemtableSizeType,
		)
	case "btree":
		return memtable.NewBTreeMemtable(
			cfg.MemtableMaxEntries,
			cfg.MemtableMaxSizeKB,
			cfg.MemtableSizeType,
		)
	default:
		return memtable.NewHashMapMemtable(
			cfg.MemtableMaxEntries,
			cfg.MemtableMaxSizeKB,
			cfg.MemtableSizeType,
		)
	}
}
