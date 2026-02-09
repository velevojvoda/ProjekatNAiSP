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
	w, err := wal.NewWAL(cfg.WALDir)
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
	e.wal.Close()
}
