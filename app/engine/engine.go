package engine

import (
	"ProjekatNAiSP/app/block"
	"ProjekatNAiSP/app/cache"
	"ProjekatNAiSP/app/config"
	"ProjekatNAiSP/app/wal"
)

type Engine struct {
	cfg          *config.Config
	data         map[string][]byte
	cache        *cache.LRUCache
	wal          *wal.WAL
	blockManager *block.BlockManager
}

func NewEngine(cfg *config.Config) (*Engine, error) {
	w, err := wal.NewWAL(cfg.WALDir, cfg.BlockSizeKB*1024, cfg.WALSegmentBlocks)
	if err != nil {
		return nil, err
	}

	bm, err := block.NewBlockManager(cfg.BlockSizeKB, cfg.CacheCapacity)
	if err != nil {
		return nil, err
	}

	e := &Engine{
		cfg:          cfg,
		data:         make(map[string][]byte),
		wal:          w,
		cache:        cache.NewLRUCache(cfg.CacheCapacity),
		blockManager: bm,
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
			e.data[rec.Key] = rec.Value
			e.cache.Delete(rec.Key)
		case wal.OpDelete:
			delete(e.data, rec.Key)
			e.cache.Delete(rec.Key)
		}
	}

	return nil
}

func (e *Engine) Put(key string, value []byte) error {
	if err := e.wal.AppendPut(key, value); err != nil {
		return err
	}
	e.data[key] = value
	e.cache.Put(key, value)
	return nil
}

func (e *Engine) Get(key string) ([]byte, error) {
	if value, ok := e.cache.Get(key); ok {
		return value, nil
	}

	value, ok := e.data[key]
	if !ok {
		return nil, nil
	}
	e.cache.Put(key, value)
	return value, nil
}

func (e *Engine) Delete(key string) error {
	if err := e.wal.AppendDelete(key); err != nil {
		return err
	}
	delete(e.data, key)
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
