package sstable

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

func writeIndexFile(path string, entries []IndexEntry) ([]SummaryEntry, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	out := make([]SummaryEntry, 0, len(entries))
	var indexOffset int64
	for _, entry := range entries {
		keyBytes := []byte(entry.Key)
		buf := make([]byte, 12+len(keyBytes))
		binary.LittleEndian.PutUint32(buf[0:4], uint32(len(keyBytes)))
		binary.LittleEndian.PutUint64(buf[4:12], uint64(entry.DataOffset))
		copy(buf[12:], keyBytes)

		if _, err := w.Write(buf); err != nil {
			return nil, err
		}
		out = append(out, SummaryEntry{Key: entry.Key, IndexOffset: indexOffset})
		indexOffset += int64(len(buf))
	}
	return out, nil
}

func readIndexEntryAt(f *os.File, offset int64) (IndexEntry, int64, error) {
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return IndexEntry{}, 0, err
	}

	var header [12]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		return IndexEntry{}, 0, err
	}

	keyLen := binary.LittleEndian.Uint32(header[0:4])
	dataOffset := binary.LittleEndian.Uint64(header[4:12])

	keyBytes := make([]byte, keyLen)
	if _, err := io.ReadFull(f, keyBytes); err != nil {
		return IndexEntry{}, 0, err
	}

	return IndexEntry{Key: string(keyBytes), DataOffset: int64(dataOffset)}, int64(12 + len(keyBytes)), nil
}
