package model

type Record struct {
	Key       string
	Value     []byte
	Timestamp uint64
	Tombstone bool
}
