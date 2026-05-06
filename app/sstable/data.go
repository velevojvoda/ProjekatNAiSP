package sstable

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"ProjekatNAiSP/app/block"
	"ProjekatNAiSP/app/model"
)

func writeDataFile(bm *block.BlockManager, path string, blockSize int, records []model.Record) ([]IndexEntry, [][]byte, error) {
	var buf []byte
	var offset int64
	indexEntries := make([]IndexEntry, 0, len(records))
	values := make([][]byte, 0, len(records))

	for _, rec := range records {
		encoded, err := encodeDataRecord(rec)
		if err != nil {
			return nil, nil, err
		}
		buf = append(buf, encoded...)
		indexEntries = append(indexEntries, IndexEntry{Key: rec.Key, DataOffset: offset})
		values = append(values, append([]byte(nil), rec.Value...))
		offset += int64(len(encoded))
	}

	if err := writeAllBytes(bm, path, blockSize, buf); err != nil {
		return nil, nil, err
	}
	return indexEntries, values, nil
}

func encodeDataRecord(rec model.Record) ([]byte, error) {
	keyBytes := []byte(rec.Key)
	valueBytes := rec.Value
	payloadLen := 1 + 4 + 4 + len(keyBytes) + len(valueBytes)
	buf := make([]byte, 4+payloadLen)

	if rec.Tombstone {
		buf[4] = 1
	}
	binary.LittleEndian.PutUint32(buf[5:9], uint32(len(keyBytes)))
	binary.LittleEndian.PutUint32(buf[9:13], uint32(len(valueBytes)))
	copy(buf[13:13+len(keyBytes)], keyBytes)
	copy(buf[13+len(keyBytes):], valueBytes)

	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], crc)
	return buf, nil
}

func decodeDataRecordAt(bm *block.BlockManager, path string, blockSize int, offset int64) (model.Record, int64, error) {
	header, err := readBytesAt(bm, path, blockSize, offset, 13)
	if err != nil {
		return model.Record{}, 0, err
	}

	// Ako su prva 4 bajta (CRC) nule — to je padding na kraju bloka, tretiramo kao EOF
	if header[0] == 0 && header[1] == 0 && header[2] == 0 && header[3] == 0 {
		return model.Record{}, 0, io.EOF
	}

	storedCRC := binary.LittleEndian.Uint32(header[0:4])
	tombstone := header[4] == 1
	keyLen := binary.LittleEndian.Uint32(header[5:9])
	valueLen := binary.LittleEndian.Uint32(header[9:13])

	payload, err := readBytesAt(bm, path, blockSize, offset+13, int(keyLen)+int(valueLen))
	if err != nil {
		return model.Record{}, 0, err
	}

	crcBuf := make([]byte, 9+len(payload))
	copy(crcBuf[:9], header[4:13])
	copy(crcBuf[9:], payload)
	if crc32.ChecksumIEEE(crcBuf) != storedCRC {
		return model.Record{}, 0, fmt.Errorf("%w at data offset %d", ErrCorruptedData, offset)
	}

	key := string(payload[:keyLen])
	value := append([]byte(nil), payload[keyLen:]...)
	return model.Record{Key: key, Value: value, Tombstone: tombstone}, int64(13 + len(payload)), nil
}

func scanDataFile(bm *block.BlockManager, path string, blockSize int, fn func(idx int, rec model.Record, offset int64) error) error {
	var offset int64
	idx := 0
	for {
		rec, size, err := decodeDataRecordAt(bm, path, blockSize, offset)
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
