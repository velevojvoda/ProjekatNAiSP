package engine

import (
	"ProjekatNAiSP/app/config"
	"ProjekatNAiSP/app/memtable"
	"ProjekatNAiSP/app/model"
	"ProjekatNAiSP/app/wal"
)

type FlushFunc func(records []model.Record) error

type Engine struct {
	cfg       *config.Config
	memtables []memtable.Memtable
	wal       *wal.WAL
	flushFn   FlushFunc
}

func NewEngine(cfg *config.Config) (*Engine, error) {
	w, err := wal.NewWAL(cfg.WALDir)
	if err != nil {
		return nil, err
	}

	activeMem := createMemtable(cfg)

	e := &Engine{
		cfg:       cfg,
		memtables: []memtable.Memtable{activeMem},
		wal:       w,
		flushFn: func(records []model.Record) error {
			return nil
		},
	}

	records, err := w.Replay()
	if err != nil {
		return nil, err
	}

	for _, record := range records {
		if record.Tombstone {
			if err := e.applyDelete(record.Key); err != nil {
				return nil, err
			}
		} else {
			if err := e.applyPut(record.Key, record.Value); err != nil {
				return nil, err
			}
		}
	}

	return e, nil
}

func (e *Engine) SetFlushFunc(fn FlushFunc) {
	if fn != nil {
		e.flushFn = fn
	}
}

func (e *Engine) activeMemtable() memtable.Memtable {
	return e.memtables[len(e.memtables)-1]
}

func (e *Engine) allMemtablesFull() bool {
	if len(e.memtables) < e.cfg.MemtableCount {
		return false
	}

	for _, mem := range e.memtables {
		if !mem.IsFull() {
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

func (e *Engine) rotateMemtableIfNeeded() error {
	active := e.activeMemtable()
	if !active.IsFull() {
		return nil
	}

	if len(e.memtables) < e.cfg.MemtableCount {
		newMem := createMemtable(e.cfg)
		e.memtables = append(e.memtables, newMem)
		return nil
	}

	return e.flushIfNeeded()
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

func (e *Engine) Put(key string, value []byte) error {
	if err := e.wal.AppendPut(key, value); err != nil {
		return err
	}
	return e.applyPut(key, value)
}

func (e *Engine) Get(key string) ([]byte, error) {
	for i := len(e.memtables) - 1; i >= 0; i-- {
		record, ok := e.memtables[i].Get(key)
		if !ok {
			continue
		}

		if record.Tombstone {
			return nil, nil
		}

		return record.Value, nil
	}

	return nil, nil
}

func (e *Engine) Delete(key string) error {
	if err := e.wal.AppendDelete(key); err != nil {
		return err
	}
	return e.applyDelete(key)
}

func (e *Engine) Shutdown() {
	e.wal.Close()
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
