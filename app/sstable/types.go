package sstable

import "ProjekatNAiSP/app/model"

type BuildOptions struct {
	Dir         string
	BlockSize   int
	SummaryStep int
	BloomM      uint64
	BloomK      uint32
	TableID     string
}

type Table struct {
	ID          string
	Dir         string
	DataPath    string
	IndexPath   string
	SummaryPath string
	FilterPath  string
	MerklePath  string
	BlockSize   int
}

type IndexEntry struct {
	Key        string
	DataOffset int64
}

type SummaryEntry struct {
	Key         string
	IndexOffset int64
}

type SummaryHeader struct {
	MinKey      string
	MaxKey      string
	SummaryStep int
}

type ValidationResult struct {
	Valid          bool
	RootMatch      bool
	MismatchedKeys []string
	MismatchedAt   []int
}

type GetResult struct {
	Record model.Record
	Found  bool
}
