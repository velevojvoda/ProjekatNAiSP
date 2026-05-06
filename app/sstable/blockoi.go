package sstable

import (
	"errors"
	"io"
	"os"

	"ProjekatNAiSP/app/block"
)

// readBytesAt čita tačno n bajtova počevši od byte-offseta 'offset' u fajlu,
// rutiranjem kroz BlockManager. Vraća io.EOF ako nema više podataka na toj
// poziciji, io.ErrUnexpectedEOF ako je zapis krnji.
func readBytesAt(bm *block.BlockManager, path string, blockSize int, offset int64, n int) ([]byte, error) {
	result := make([]byte, 0, n)
	for len(result) < n {
		current := offset + int64(len(result))
		blockNum := current / int64(blockSize)
		offsetInBlock := int(current % int64(blockSize))

		blockData, err := bm.ReadBlock(path, blockNum)
		if err != nil {
			if errors.Is(err, block.ErrPartialBlockRead) {
				if len(result) == 0 {
					return nil, io.EOF
				}
				return result, io.ErrUnexpectedEOF
			}
			return nil, err
		}

		available := blockSize - offsetInBlock
		needed := n - len(result)
		toCopy := min(available, needed)
		result = append(result, blockData[offsetInBlock:offsetInBlock+toCopy]...)
	}
	return result, nil
}

// writeAllBytes piše sve bajtove iz 'data' u fajl blok-po-blok kroz BlockManager.
// Poslednji blok se dopunjuje nulama do pune veličine bloka.
func writeAllBytes(bm *block.BlockManager, path string, blockSize int, data []byte) error {
	if len(data) == 0 {
		// Zapiši prazan blok da fajl postoji na disku
		empty := make([]byte, blockSize)
		return bm.WriteBlock(path, 0, empty)
	}

	numFullBlocks := len(data) / blockSize
	for i := 0; i < numFullBlocks; i++ {
		chunk := make([]byte, blockSize)
		copy(chunk, data[i*blockSize:(i+1)*blockSize])
		if err := bm.WriteBlock(path, int64(i), chunk); err != nil {
			return err
		}
	}

	if len(data)%blockSize != 0 {
		last := make([]byte, blockSize)
		copy(last, data[numFullBlocks*blockSize:])
		if err := bm.WriteBlock(path, int64(numFullBlocks), last); err != nil {
			return err
		}
	}

	return nil
}

// readAllBytes čita sav sadržaj fajla blok-po-blok kroz BlockManager.
// Koristi os.Stat samo za dužinu (ne čita sadržaj direktno).
func readAllBytes(bm *block.BlockManager, path string, blockSize int) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	fileSize := info.Size()
	if fileSize == 0 {
		return []byte{}, nil
	}

	result := make([]byte, 0, fileSize)
	blockNum := int64(0)
	for int64(len(result)) < fileSize {
		blockData, err := bm.ReadBlock(path, blockNum)
		if err != nil {
			return nil, err
		}
		remaining := fileSize - int64(len(result))
		if remaining < int64(blockSize) {
			result = append(result, blockData[:remaining]...)
		} else {
			result = append(result, blockData...)
		}
		blockNum++
	}
	return result, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
