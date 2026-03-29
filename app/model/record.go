package model

type Record struct {
	Key       string
	Value     []byte
	Tombstone bool
}
