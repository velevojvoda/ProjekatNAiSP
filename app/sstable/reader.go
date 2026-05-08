package sstable

import (
	"io"

	"ProjekatNAiSP/app/model"
)


func (t *Table) Get(key string) (GetResult, error) {
	bf, err := readBloomFilter(t.BM, t.FilterPath, t.BlockSize)
	if err != nil {
		return GetResult{}, err
	}
	if !bf.MightContain(key) {
		return GetResult{Found: false}, nil
	}

	startOffset, err := findIndexStartOffsetLazy(t.BM, t.SummaryPath, t.BlockSize, key)
	if err != nil {
		if err == ErrInvalidSummaryRange || err == ErrNotFound {
			return GetResult{Found: false}, nil
		}
		return GetResult{}, err
	}

	var dataOffset int64 = -1
	currentOffset := startOffset
	for {
		entry, size, err := readIndexEntryAt(t.BM, t.IndexPath, t.BlockSize, currentOffset)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			break
		}
		if entry.Key == key {
			dataOffset = entry.DataOffset
			break
		}
		if entry.Key > key {
			break
		}
		currentOffset += size
	}

	if dataOffset == -1 {
		return GetResult{Found: false}, nil
	}

	rec, _, err := decodeDataRecordAt(t.BM, t.DataPath, t.BlockSize, dataOffset)
	if err != nil {
		return GetResult{}, err
	}
	return GetResult{Record: rec, Found: true}, nil
}

func (t *Table) AllRecords() ([]model.Record, error) {
	out := make([]model.Record, 0)
	err := scanDataFile(t.BM, t.DataPath, t.BlockSize, func(_ int, rec model.Record, _ int64) error {
		out = append(out, rec)
		return nil
	})
	return out, err
}
