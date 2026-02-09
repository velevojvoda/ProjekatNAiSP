package wal

import (
	"encoding/binary"
	"fmt"
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
