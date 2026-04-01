package sstable

import "errors"

var (
	ErrNotFound            = errors.New("sstable: key not found")
	ErrCorruptedData       = errors.New("sstable: corrupted data")
	ErrInvalidSummaryRange = errors.New("sstable: key outside summary range")
)
