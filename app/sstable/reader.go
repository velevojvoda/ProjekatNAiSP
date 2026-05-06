package sstable

import (
	"io"
	"os"

	"ProjekatNAiSP/app/model"
)

// Get prati read path iz specifikacije za jednu SSTable:
//  1. Bloom filter — ako kaže "ne postoji", izlazimo.
//  2. Summary — proveravamo opseg [MinKey, MaxKey] i nalazimo startni offset
//     u Index fajlu (lazy, bez učitavanja celog summary-ja).
//  3. Index — od tog offset-a sekvencijalno čitamo ulaze dok ne nađemo ključ
//     ili ne pređemo poziciju gde bi ključ trebalo da bude.
//  4. Data — sa offset-a iz index-a čitamo konkretan zapis i validiramo CRC.
func (t *Table) Get(key string) (GetResult, error) {
	bf, err := readBloomFilter(t.FilterPath)
	if err != nil {
		return GetResult{}, err
	}
	if !bf.MightContain(key) {
		return GetResult{Found: false}, nil
	}

	startOffset, err := findIndexStartOffsetLazy(t.SummaryPath, key)
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

// AllRecords sekvencijalno pročita sve zapise iz data.db. Koristi se u Merkle
// validaciji kada želimo da iznova izračunamo stablo.
func (t *Table) AllRecords() ([]model.Record, error) {
	out := make([]model.Record, 0)
	err := scanDataFile(t.DataPath, func(_ int, rec model.Record, _ int64) error {
		out = append(out, rec)
		return nil
	})
	return out, err
}
