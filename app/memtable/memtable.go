package memtable

import "ProjekatNAiSP/app/model"

type Memtable interface {
	Put(key string, value []byte) error
	Get(key string) (*model.Record, bool)
	Delete(key string) error
}
