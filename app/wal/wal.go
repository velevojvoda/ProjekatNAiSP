package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	OpPut    byte = 1
	OpDelete byte = 2
)

type Record struct {
	Op    byte
	Key   string
	Value []byte
}

type WAL struct {
	dir              string
	currentFile      *os.File
	currentPath      string
	currentSegment   int
	recordsInSegment int
	maxRecordsPerSeg int
}

func NewWAL(dir string, maxRecordsPerSeg int) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	if maxRecordsPerSeg <= 0 {
		maxRecordsPerSeg = 100
	}

	w := &WAL{
		dir:              dir,
		maxRecordsPerSeg: maxRecordsPerSeg,
	}

	if err := w.initializeLastSegment(); err != nil {
		return nil, err
	}

	return w, nil
}

func (w *WAL) initializeLastSegment() error {
	segments, err := w.listSegments()
	if err != nil {
		return err
	}

	// Ako nema nijednog segmenta, kreiraj prvi
	if len(segments) == 0 {
		w.currentSegment = 1
		w.currentPath = w.segmentPath(w.currentSegment)

		f, err := os.OpenFile(w.currentPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return fmt.Errorf("can't create first WAL segment: %w", err)
		}

		w.currentFile = f
		w.recordsInSegment = 0
		return nil
	}

	// Inače uzmi poslednji segment
	lastPath := segments[len(segments)-1]
	lastSegmentNumber, err := extractSegmentNumber(lastPath)
	if err != nil {
		return err
	}

	recordCount, err := countRecordsInFile(lastPath)
	if err != nil {
		return err
	}

	// Ako je poslednji segment pun, napravi novi
	if recordCount >= w.maxRecordsPerSeg {
		w.currentSegment = lastSegmentNumber + 1
		w.currentPath = w.segmentPath(w.currentSegment)

		f, err := os.OpenFile(w.currentPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return fmt.Errorf("can't create next WAL segment: %w", err)
		}

		w.currentFile = f
		w.recordsInSegment = 0
		return nil
	}

	// Ako nije pun, nastavi da pišeš u njega
	f, err := os.OpenFile(lastPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("can't open existing WAL segment: %w", err)
	}

	w.currentFile = f
	w.currentPath = lastPath
	w.currentSegment = lastSegmentNumber
	w.recordsInSegment = recordCount

	return nil
}

func (w *WAL) AppendPut(key string, value []byte) error {
	var payload bytes.Buffer

	if err := payload.WriteByte(OpPut); err != nil {
		return err
	}

	if err := binary.Write(&payload, binary.LittleEndian, uint32(len(key))); err != nil {
		return err
	}

	if err := binary.Write(&payload, binary.LittleEndian, uint32(len(value))); err != nil {
		return err
	}

	if _, err := payload.Write([]byte(key)); err != nil {
		return err
	}

	if _, err := payload.Write(value); err != nil {
		return err
	}

	if err := w.appendPayload(payload.Bytes()); err != nil {
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

	if err := w.appendPayload(payload.Bytes()); err != nil {
		return err
	}

	return nil
}

func (w *WAL) appendPayload(payload []byte) error {
	// Ako je segment pun, rotiraj
	if w.recordsInSegment >= w.maxRecordsPerSeg {
		if err := w.rotateSegment(); err != nil {
			return err
		}
	}

	crc := crc32.ChecksumIEEE(payload)

	if err := binary.Write(w.currentFile, binary.LittleEndian, crc); err != nil {
		return err
	}

	if _, err := w.currentFile.Write(payload); err != nil {
		return err
	}

	w.recordsInSegment++
	return nil
}

func (w *WAL) rotateSegment() error {
	if w.currentFile != nil {
		if err := w.currentFile.Close(); err != nil {
			return err
		}
	}

	w.currentSegment++
	w.currentPath = w.segmentPath(w.currentSegment)

	f, err := os.OpenFile(w.currentPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}

	w.currentFile = f
	w.recordsInSegment = 0
	return nil
}

func (w *WAL) Close() error {
	if w.currentFile == nil {
		return nil
	}
	return w.currentFile.Close()
}

func (w *WAL) ReadAllRecords() ([]Record, error) {
	segments, err := w.listSegments()
	if err != nil {
		return nil, err
	}

	var all []Record

	for _, segPath := range segments {
		f, err := os.Open(segPath)
		if err != nil {
			return nil, err
		}

		for {
			rec, err := ReadNextRecord(f)
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = f.Close()
				return nil, fmt.Errorf("error while reading segment %s: %w", segPath, err)
			}

			all = append(all, rec)
		}

		_ = f.Close()
	}

	return all, nil
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

func (w *WAL) listSegments() ([]string, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, err
	}

	var segments []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if strings.HasPrefix(name, "wal_") && strings.HasSuffix(name, ".log") {
			segments = append(segments, filepath.Join(w.dir, name))
		}
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i] < segments[j]
	})

	return segments, nil
}

func (w *WAL) segmentPath(segment int) string {
	return filepath.Join(w.dir, fmt.Sprintf("wal_%04d.log", segment))
}

func extractSegmentNumber(path string) (int, error) {
	base := filepath.Base(path)

	var num int
	_, err := fmt.Sscanf(base, "wal_%04d.log", &num)
	if err != nil {
		return 0, fmt.Errorf("invalid WAL segment name: %s", base)
	}

	return num, nil
}

func countRecordsInFile(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	count := 0
	for {
		_, err := ReadNextRecord(f)
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		count++
	}

	return count, nil
}
