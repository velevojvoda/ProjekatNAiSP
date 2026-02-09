package engine

import (
	"ProjekatNAiSP/app/config"
	"errors"
)

type Engine struct {
	cfg  *config.Config
	data map[string][]byte
}

func NewEngine(cfg *config.Config) (*Engine, error) {
	e := &Engine{
		cfg:  cfg,
		data: make(map[string][]byte),
	}
	return e, nil
}

func (e *Engine) Put(key string, value []byte) error {
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
	_, ok := e.data[key]
	if !ok {
		return errors.New("key not found")
	}
	delete(e.data, key)
	return nil
}

func Shutdown() {}
