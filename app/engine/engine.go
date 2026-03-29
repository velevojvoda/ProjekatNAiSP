package engine

import (
	"ProjekatNAiSP/app/config"
	"ProjekatNAiSP/app/wal"
)

type Engine struct {
	cfg  *config.Config
	data map[string][]byte
	wal  *wal.WAL
}

func NewEngine(cfg *config.Config) (*Engine, error) {
	w, err := wal.NewWAL(cfg.WALDir, cfg.BlockSizeKB*1024, cfg.WALSegmentBlocks)
	if err != nil {
		return nil, err
	}

	e := &Engine{
		cfg:  cfg,
		data: make(map[string][]byte),
		wal:  w,
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
		case wal.OpDelete:
			delete(e.data, rec.Key)
		}
	}

	return nil
}

func (e *Engine) Put(key string, value []byte) error {
	if err := e.wal.AppendPut(key, value); err != nil {
		return err
	}
	e.data[key] = value
	return nil
}

func (e *Engine) Get(key string) ([]byte, error) {
	value, ok := e.data[key]
	if !ok {
		return nil, nil
	}
	return value, nil
}

func (e *Engine) Delete(key string) error {
	if err := e.wal.AppendDelete(key); err != nil {
		return err
	}
	delete(e.data, key)
	return nil
}

func (e *Engine) Shutdown() {
	_ = e.wal.Close()
}
