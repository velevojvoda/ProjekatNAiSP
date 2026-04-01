package sstable

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"ProjekatNAiSP/app/model"
)

func writeDataFile(path string, records []model.Record) ([]IndexEntry, [][]byte, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	var offset int64
	indexEntries := make([]IndexEntry, 0, len(records))
	values := make([][]byte, 0, len(records))

	for _, rec := range records {
		encoded, err := encodeDataRecord(rec)
		if err != nil {
			return nil, nil, err
		}
		if _, err := w.Write(encoded); err != nil {
			return nil, nil, err
		}
		indexEntries = append(indexEntries, IndexEntry{Key: rec.Key, DataOffset: offset})
		values = append(values, append([]byte(nil), rec.Value...))
		offset += int64(len(encoded))
	}
	return indexEntries, values, nil
}

func encodeDataRecord(rec model.Record) ([]byte, error) {
	keyBytes := []byte(rec.Key)
	valueBytes := rec.Value
	payloadLen := 8 + 1 + 4 + 4 + len(keyBytes) + len(valueBytes)
	buf := make([]byte, 4+payloadLen)

	binary.LittleEndian.PutUint64(buf[4:12], rec.Timestamp)
	if rec.Tombstone {
		buf[12] = 1
	}
	binary.LittleEndian.PutUint32(buf[13:17], uint32(len(keyBytes)))
	binary.LittleEndian.PutUint32(buf[17:21], uint32(len(valueBytes)))
	copy(buf[21:21+len(keyBytes)], keyBytes)
	copy(buf[21+len(keyBytes):], valueBytes)

	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], crc)
	return buf, nil
}

func decodeDataRecordAt(f *os.File, offset int64) (model.Record, int64, error) {
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return model.Record{}, 0, err
	}

	var header [21]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		return model.Record{}, 0, err
	}

	storedCRC := binary.LittleEndian.Uint32(header[0:4])
	timestamp := binary.LittleEndian.Uint64(header[4:12])
	tombstone := header[12] == 1
	keyLen := binary.LittleEndian.Uint32(header[13:17])
	valueLen := binary.LittleEndian.Uint32(header[17:21])

	payload := make([]byte, int(keyLen)+int(valueLen))
	if _, err := io.ReadFull(f, payload); err != nil {
		return model.Record{}, 0, err
	}

	crcBuf := make([]byte, 17+len(payload))
	copy(crcBuf[:17], header[4:21])
	copy(crcBuf[17:], payload)
	if crc32.ChecksumIEEE(crcBuf) != storedCRC {
		return model.Record{}, 0, fmt.Errorf("%w at data offset %d", ErrCorruptedData, offset)
	}

	key := string(payload[:keyLen])
	value := append([]byte(nil), payload[keyLen:]...)
	return model.Record{Key: key, Value: value, Timestamp: timestamp, Tombstone: tombstone}, int64(21 + len(payload)), nil
}

func scanDataFile(path string, fn func(idx int, rec model.Record, offset int64) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var offset int64
	idx := 0
	for {
		rec, size, err := decodeDataRecordAt(f, offset)
		if err == io.EOF {
			return nil
		}
		if err == io.ErrUnexpectedEOF {
			return fmt.Errorf("%w at end of data file", ErrCorruptedData)
		}
		if err != nil {
			return err
		}
		if err := fn(idx, rec, offset); err != nil {
			return err
		}
		offset += size
		idx++
	}
}
