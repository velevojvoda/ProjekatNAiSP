package engine

import (
	"path/filepath"
	"time"

	"ProjekatNAiSP/app/cache"
	"ProjekatNAiSP/app/config"
	"ProjekatNAiSP/app/model"
	"ProjekatNAiSP/app/sstable"
	"ProjekatNAiSP/app/wal"
)

type Engine struct {
	cfg            *config.Config
	data           map[string]model.Record
	cache          *cache.LRUCache
	wal            *wal.WAL
	sstableManager *sstable.Manager
	tables         []*sstable.Table
}

func NewEngine(cfg *config.Config) (*Engine, error) {
	w, err := wal.NewWAL(cfg.WALDir, cfg.BlockSizeKB*1024, cfg.WALSegmentBlocks)
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

	return &Engine{
		cfg:            cfg,
		data:           make(map[string]model.Record),
		wal:            w,
		cache:          cache.NewLRUCache(cfg.CacheCapacity),
		sstableManager: mgr,
		tables:         tables,
	}, nil
}

func (e *Engine) Recover() error {
	records, err := e.wal.ReadAllRecords()
	if err != nil {
		return err
	}

	for _, rec := range records {
		switch rec.Op {
		case wal.OpPut:
			e.data[rec.Key] = model.Record{Key: rec.Key, Value: append([]byte(nil), rec.Value...), Timestamp: uint64(time.Now().UnixNano()), Tombstone: false}
			e.cache.Delete(rec.Key)
		case wal.OpDelete:
			e.data[rec.Key] = model.Record{Key: rec.Key, Timestamp: uint64(time.Now().UnixNano()), Tombstone: true}
			e.cache.Delete(rec.Key)
		}
	}
	return nil
}

func (e *Engine) Put(key string, value []byte) error {
	if err := e.wal.AppendPut(key, value); err != nil {
		return err
	}
	e.data[key] = model.Record{Key: key, Value: append([]byte(nil), value...), Timestamp: uint64(time.Now().UnixNano()), Tombstone: false}
	e.cache.Put(key, value)
	if len(e.data) >= e.cfg.MemtableMaxEntries {
		if err := e.flushToSSTable(); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) Get(key string) ([]byte, error) {
	if rec, ok := e.data[key]; ok {
		if rec.Tombstone {
			return nil, nil
		}
		e.cache.Put(key, rec.Value)
		return append([]byte(nil), rec.Value...), nil
	}

	if value, ok := e.cache.Get(key); ok {
		return value, nil
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
	e.data[key] = model.Record{Key: key, Timestamp: uint64(time.Now().UnixNano()), Tombstone: true}
	e.cache.Delete(key)
	if len(e.data) >= e.cfg.MemtableMaxEntries {
		if err := e.flushToSSTable(); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) Shutdown() {
	_ = e.wal.Close()
}

func (e *Engine) flushToSSTable() error {
	if len(e.data) == 0 {
		return nil
	}

	records := make([]model.Record, 0, len(e.data))
	for _, rec := range e.data {
		records = append(records, rec)
	}

	table, err := e.sstableManager.BuildFromRecords(records)
	if err != nil {
		return err
	}

	e.tables = append(e.tables, table)
	e.data = make(map[string]model.Record)
	return nil
}
