package sstable

import (
	"encoding/binary"
	"io"

	"ProjekatNAiSP/app/block"
)

func writeIndexFile(bm *block.BlockManager, path string, blockSize int, entries []IndexEntry) ([]SummaryEntry, error) {
	var buf []byte
	out := make([]SummaryEntry, 0, len(entries))
	var indexOffset int64

	for _, entry := range entries {
		keyBytes := []byte(entry.Key)
		b := make([]byte, 12+len(keyBytes))
		binary.LittleEndian.PutUint32(b[0:4], uint32(len(keyBytes)))
		binary.LittleEndian.PutUint64(b[4:12], uint64(entry.DataOffset))
		copy(b[12:], keyBytes)

		buf = append(buf, b...)
		out = append(out, SummaryEntry{Key: entry.Key, IndexOffset: indexOffset})
		indexOffset += int64(len(b))
	}

	if err := writeAllBytes(bm, path, blockSize, buf); err != nil {
		return nil, err
	}
	return out, nil
}

func readIndexEntryAt(bm *block.BlockManager, path string, blockSize int, offset int64) (IndexEntry, int64, error) {
	header, err := readBytesAt(bm, path, blockSize, offset, 12)
	if err != nil {
		return IndexEntry{}, 0, err
	}

	keyLen := binary.LittleEndian.Uint32(header[0:4])
	dataOffset := binary.LittleEndian.Uint64(header[4:12])

	keyBytes, err := readBytesAt(bm, path, blockSize, offset+12, int(keyLen))
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return IndexEntry{}, 0, err
	}

	return IndexEntry{Key: string(keyBytes), DataOffset: int64(dataOffset)}, int64(12 + len(keyBytes)), nil
}
