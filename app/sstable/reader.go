package sstable

import (
	"io"
	"os"

	"ProjekatNAiSP/app/model"
)

func (t *Table) Get(key string) (GetResult, error) {
	bf, err := readBloomFilter(t.FilterPath)
	if err != nil {
		return GetResult{}, err
	}
	if !bf.MightContain(key) {
		return GetResult{Found: false}, nil
	}

	header, summaryEntries, err := readSummary(t.SummaryPath)
	if err != nil {
		return GetResult{}, err
	}
	startOffset, err := findIndexStartOffset(header, summaryEntries, key)
	if err != nil {
		if err == ErrInvalidSummaryRange || err == ErrNotFound {
			return GetResult{Found: false}, nil
		}
		return GetResult{}, err
	}

	indexFile, err := os.Open(t.IndexPath)
	if err != nil {
		return GetResult{}, err
	}
	defer indexFile.Close()

	var dataOffset int64 = -1
	currentOffset := startOffset
	for {
		entry, size, err := readIndexEntryAt(indexFile, currentOffset)
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

	dataFile, err := os.Open(t.DataPath)
	if err != nil {
		return GetResult{}, err
	}
	defer dataFile.Close()

	rec, _, err := decodeDataRecordAt(dataFile, dataOffset)
	if err != nil {
		return GetResult{}, err
	}
	return GetResult{Record: rec, Found: true}, nil
}

func (t *Table) AllRecords() ([]model.Record, error) {
	out := make([]model.Record, 0)
	err := scanDataFile(t.DataPath, func(_ int, rec model.Record, _ int64) error {
		out = append(out, rec)
		return nil
	})
	return out, err
}
