package sstable

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

func writeSummaryFile(path string, indexEntries []SummaryEntry, step int) (SummaryHeader, error) {
	f, err := os.Create(path)
	if err != nil {
		return SummaryHeader{}, err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	header := SummaryHeader{SummaryStep: step}
	if len(indexEntries) > 0 {
		header.MinKey = indexEntries[0].Key
		header.MaxKey = indexEntries[len(indexEntries)-1].Key
	}

	if err := writeSummaryHeader(w, header); err != nil {
		return SummaryHeader{}, err
	}

	for i, entry := range indexEntries {
		if i%step != 0 && i != len(indexEntries)-1 {
			continue
		}
		if err := writeSummaryEntry(w, entry); err != nil {
			return SummaryHeader{}, err
		}
	}

	return header, nil
}

func writeSummaryHeader(w *bufio.Writer, h SummaryHeader) error {
	minBytes := []byte(h.MinKey)
	maxBytes := []byte(h.MaxKey)

	buf := make([]byte, 12+len(minBytes)+len(maxBytes))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(minBytes)))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(maxBytes)))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(h.SummaryStep))
	copy(buf[12:12+len(minBytes)], minBytes)
	copy(buf[12+len(minBytes):], maxBytes)

	_, err := w.Write(buf)
	return err
}

func writeSummaryEntry(w *bufio.Writer, entry SummaryEntry) error {
	keyBytes := []byte(entry.Key)
	buf := make([]byte, 12+len(keyBytes))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(keyBytes)))
	binary.LittleEndian.PutUint64(buf[4:12], uint64(entry.IndexOffset))
	copy(buf[12:], keyBytes)
	_, err := w.Write(buf)
	return err
}

func readSummary(path string) (SummaryHeader, []SummaryEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return SummaryHeader{}, nil, err
	}
	defer f.Close()

	var hbuf [12]byte
	if _, err := io.ReadFull(f, hbuf[:]); err != nil {
		return SummaryHeader{}, nil, err
	}

	minLen := binary.LittleEndian.Uint32(hbuf[0:4])
	maxLen := binary.LittleEndian.Uint32(hbuf[4:8])
	step := binary.LittleEndian.Uint32(hbuf[8:12])

	keyBytes := make([]byte, int(minLen)+int(maxLen))
	if _, err := io.ReadFull(f, keyBytes); err != nil {
		return SummaryHeader{}, nil, err
	}

	header := SummaryHeader{MinKey: string(keyBytes[:minLen]), MaxKey: string(keyBytes[minLen:]), SummaryStep: int(step)}
	entries := make([]SummaryEntry, 0)
	for {
		var eh [12]byte
		_, err := io.ReadFull(f, eh[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return SummaryHeader{}, nil, err
		}

		keyLen := binary.LittleEndian.Uint32(eh[0:4])
		indexOffset := binary.LittleEndian.Uint64(eh[4:12])
		kb := make([]byte, keyLen)
		if _, err := io.ReadFull(f, kb); err != nil {
			return SummaryHeader{}, nil, err
		}

		entries = append(entries, SummaryEntry{Key: string(kb), IndexOffset: int64(indexOffset)})
	}

	return header, entries, nil
}

func findIndexStartOffset(header SummaryHeader, entries []SummaryEntry, key string) (int64, error) {
	if len(entries) == 0 {
		return 0, ErrNotFound
	}
	if header.MinKey != "" && (key < header.MinKey || key > header.MaxKey) {
		return 0, ErrInvalidSummaryRange
	}

	candidate := entries[0].IndexOffset
	for _, entry := range entries {
		if entry.Key <= key {
			candidate = entry.IndexOffset
			continue
		}
		break
	}
	return candidate, nil
}
