package wal

import (
	"ProjekatNAiSP/app/model"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

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
		return nil, fmt.Errorf("Can't open WAL")
	}

	return &WAL{
		file: f,
		path: path,
	}, nil
}

func (w *WAL) AppendPut(key string, value []byte) error {
	if _, err := w.file.Write([]byte{1}); err != nil {
		return err
	}

	// dužine
	if err := binary.Write(w.file, binary.LittleEndian, uint32(len(key))); err != nil {
		return err
	}
	if err := binary.Write(w.file, binary.LittleEndian, uint32(len(value))); err != nil {
		return err
	}

	// podaci
	if _, err := w.file.Write([]byte(key)); err != nil {
		return err
	}
	if _, err := w.file.Write(value); err != nil {
		return err
	}

	return nil
}

func (w *WAL) AppendDelete(key string) error {
	if _, err := w.file.Write([]byte{2}); err != nil {
		return err
	}

	if err := binary.Write(w.file, binary.LittleEndian, uint32(len(key))); err != nil {
		return err
	}

	if _, err := w.file.Write([]byte(key)); err != nil {
		return err
	}

	return nil
}

func (w *WAL) Close() error {
	return w.file.Close()
}
func (w *WAL) Replay() ([]model.Record, error) {
	f, err := os.Open(w.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var records []model.Record

	for {
		op := make([]byte, 1)
		_, err := f.Read(op)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		switch op[0] {
		case 1: // PUT
			var keyLen uint32
			var valueLen uint32

			if err := binary.Read(f, binary.LittleEndian, &keyLen); err != nil {
				return nil, err
			}
			if err := binary.Read(f, binary.LittleEndian, &valueLen); err != nil {
				return nil, err
			}

			key := make([]byte, keyLen)
			value := make([]byte, valueLen)

			if _, err := io.ReadFull(f, key); err != nil {
				return nil, err
			}
			if _, err := io.ReadFull(f, value); err != nil {
				return nil, err
			}

			records = append(records, model.Record{
				Key:       string(key),
				Value:     value,
				Tombstone: false,
			})

		case 2: // DELETE
			var keyLen uint32

			if err := binary.Read(f, binary.LittleEndian, &keyLen); err != nil {
				return nil, err
			}

			key := make([]byte, keyLen)
			if _, err := io.ReadFull(f, key); err != nil {
				return nil, err
			}

			records = append(records, model.Record{
				Key:       string(key),
				Value:     nil,
				Tombstone: true,
			})

		default:
			return nil, fmt.Errorf("unknown WAL operation")
		}
	}

	return records, nil
}
