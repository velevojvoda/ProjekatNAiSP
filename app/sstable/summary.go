package sstable

import (
	"encoding/binary"
	"io"

	"ProjekatNAiSP/app/block"
)

func writeSummaryFile(bm *block.BlockManager, path string, blockSize int, indexEntries []SummaryEntry, step int) (SummaryHeader, error) {
	if step <= 0 {
		step = 1
	}

	header := SummaryHeader{SummaryStep: step}
	if len(indexEntries) > 0 {
		header.MinKey = indexEntries[0].Key
		header.MaxKey = indexEntries[len(indexEntries)-1].Key
	}

	var buf []byte

	// Header
	minBytes := []byte(header.MinKey)
	maxBytes := []byte(header.MaxKey)
	hbuf := make([]byte, 12+len(minBytes)+len(maxBytes))
	binary.LittleEndian.PutUint32(hbuf[0:4], uint32(len(minBytes)))
	binary.LittleEndian.PutUint32(hbuf[4:8], uint32(len(maxBytes)))
	binary.LittleEndian.PutUint32(hbuf[8:12], uint32(step))
	copy(hbuf[12:12+len(minBytes)], minBytes)
	copy(hbuf[12+len(minBytes):], maxBytes)
	buf = append(buf, hbuf...)

	for i, entry := range indexEntries {
		if i%step != 0 && i != len(indexEntries)-1 {
			continue
		}
		keyBytes := []byte(entry.Key)
		eb := make([]byte, 12+len(keyBytes))
		binary.LittleEndian.PutUint32(eb[0:4], uint32(len(keyBytes)))
		binary.LittleEndian.PutUint64(eb[4:12], uint64(entry.IndexOffset))
		copy(eb[12:], keyBytes)
		buf = append(buf, eb...)
	}

	if err := writeAllBytes(bm, path, blockSize, buf); err != nil {
		return SummaryHeader{}, err
	}
	return header, nil
}

func findIndexStartOffsetLazy(bm *block.BlockManager, path string, blockSize int, key string) (int64, error) {
	var pos int64

	hbuf, err := readBytesAt(bm, path, blockSize, pos, 12)
	if err != nil {
		return 0, err
	}
	pos += 12

	minLen := int(binary.LittleEndian.Uint32(hbuf[0:4]))
	maxLen := int(binary.LittleEndian.Uint32(hbuf[4:8]))

	keyBuf, err := readBytesAt(bm, path, blockSize, pos, minLen+maxLen)
	if err != nil {
		return 0, err
	}
	pos += int64(minLen + maxLen)

	minKey := string(keyBuf[:minLen])
	maxKey := string(keyBuf[minLen:])

	if minKey == "" && maxKey == "" {
		return 0, ErrNotFound
	}
	if key < minKey || key > maxKey {
		return 0, ErrInvalidSummaryRange
	}

	var candidate int64 = -1
	for {
		eh, err := readBytesAt(bm, path, blockSize, pos, 12)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return 0, err
		}
		pos += 12

		keyLen := int(binary.LittleEndian.Uint32(eh[0:4]))
		indexOffset := binary.LittleEndian.Uint64(eh[4:12])

		kb, err := readBytesAt(bm, path, blockSize, pos, keyLen)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return 0, err
		}
		pos += int64(keyLen)

		entryKey := string(kb)
		if entryKey <= key {
			candidate = int64(indexOffset)
			continue
		}
		break
	}

	if candidate == -1 {
		return 0, ErrNotFound
	}
	return candidate, nil
}
