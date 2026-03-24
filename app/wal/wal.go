package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	OpPut    = 1
	OpDelete = 2
)

type Record struct {
	Op    byte
	Key   string
	Value []byte
}

type WAL struct {
	file *os.File
	path string
}

func NewWAL(dir string) (*WAL, error) {
	path := filepath.Join(dir, "wal.log")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	return &WAL{
		file: f,
		path: path}, nil
}

func (w *WAL) AppendPut(key string, value []byte) error {
	var payload bytes.Buffer

	//op
	if err := payload.WriteByte(OpPut); err != nil {
		return err
	}

	//key length
	if err := binary.Write(&payload, binary.LittleEndian, uint32(len(key))); err != nil {
		return err
	}

	//value length
	if err := binary.Write(&payload, binary.LittleEndian, uint32(len(value))); err != nil {
		return err
	}

	//key
	if _, err := payload.Write([]byte(key)); err != nil {
		return err
	}

	//value
	if _, err := payload.Write(value); err != nil {
		return err
	}

	crc := crc32.ChecksumIEEE(payload.Bytes())

	if err := binary.Write(w.file, binary.LittleEndian, crc); err != nil {
		return err
	}

	if _, err := w.file.Write(payload.Bytes()); err != nil {
		return err
	}

	return nil
}

func (w *WAL) AppendDelete(key string) error {
	var payload bytes.Buffer
	if err := payload.WriteByte(OpDelete); err != nil {
		return err
	}
	if err := binary.Write(&payload, binary.LittleEndian, uint32(len(key))); err != nil {
		return err
	}
	if _, err := payload.Write([]byte(key)); err != nil {
		return err
	}

	crc := crc32.ChecksumIEEE(payload.Bytes())

	if err := binary.Write(w.file, binary.LittleEndian, crc); err != nil {
		return err
	}

	if _, err := w.file.Write(payload.Bytes()); err != nil {
		return err
	}

	return nil
}

func (w *WAL) Close() error {
	return w.file.Close()
}

func (w *WAL) ReadAllRecords() ([]Record, error) {
	f, err := os.Open(w.path)
	if err != nil {
		return nil, fmt.Errorf("can't open WAL for reading: %w", err)
	}
	defer f.Close()

	var records []Record

	for {
		record, err := ReadNextRecord(f)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		records = append(records, record)
	}

	return records, nil
}

func ReadNextRecord(r io.Reader) (Record, error) {
	var storedCRC uint32
	if err := binary.Read(r, binary.LittleEndian, &storedCRC); err != nil {
		return Record{}, err
	}

	opBuf := make([]byte, 1)
	if _, err := io.ReadFull(r, opBuf); err != nil {
		return Record{}, err
	}
	op := opBuf[0]

	var payload bytes.Buffer
	payload.WriteByte(op)

	switch op {
	case OpPut:
		var keyLen uint32
		var valueLen uint32

		if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
			return Record{}, err
		}
		if err := binary.Read(r, binary.LittleEndian, &valueLen); err != nil {
			return Record{}, err
		}

		if err := binary.Write(&payload, binary.LittleEndian, keyLen); err != nil {
			return Record{}, err
		}
		if err := binary.Write(&payload, binary.LittleEndian, valueLen); err != nil {
			return Record{}, err
		}

		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyBytes); err != nil {
			return Record{}, err
		}
		valueBytes := make([]byte, valueLen)
		if _, err := io.ReadFull(r, valueBytes); err != nil {
			return Record{}, err
		}

		payload.Write(keyBytes)
		payload.Write(valueBytes)

		calculatedCRC := crc32.ChecksumIEEE(payload.Bytes())
		if calculatedCRC != storedCRC {
			return Record{}, fmt.Errorf("CRC mismatch: stored=%d calculated=%d", storedCRC, calculatedCRC)
		}

		return Record{
			Op:    OpPut,
			Key:   string(keyBytes),
			Value: valueBytes,
		}, nil

	case OpDelete:
		var keyLen uint32

		if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
			return Record{}, err
		}

		if err := binary.Write(&payload, binary.LittleEndian, keyLen); err != nil {
			return Record{}, err
		}

		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyBytes); err != nil {
			return Record{}, err
		}

		payload.Write(keyBytes)

		calculatedCRC := crc32.ChecksumIEEE(payload.Bytes())
		if calculatedCRC != storedCRC {
			return Record{}, fmt.Errorf("CRC mismatch: stored=%d calculated=%d", storedCRC, calculatedCRC)
		}

		return Record{
			Op:  OpDelete,
			Key: string(keyBytes),
		}, nil

	default:
		return Record{}, fmt.Errorf("unknown operation type: %d", op)
	}
}
