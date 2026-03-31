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

const (
	FragFull   byte = 1
	FragFirst  byte = 2
	FragMiddle byte = 3
	FragLast   byte = 4
)

// [recordCRC(4)][fragType(1)][payloadLen(4)][payload]
const fragmentHeaderSize = 9

// [Op][keyLen][valueLen][keyBytes][valueBytes] for put
// [Op][keyLen][keyBytes] for delete
type Record struct {
	Op    byte
	Key   string
	Value []byte
}

type fragment struct {
	recordCRC uint32
	fragType  byte
	payload   []byte
}

type WAL struct {
	dir              string
	blockSize        int
	blocksPerSegment int
	segmentSize      int

	currentFile    *os.File
	currentPath    string
	currentSegment int
	currentOffset  int64
}

func NewWAL(dir string, blockSize int, blocksPerSegment int) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	if blockSize <= 0 {
		return nil, fmt.Errorf("invalid WAL block size")
	}

	if blocksPerSegment <= 0 {
		return nil, fmt.Errorf("invalid WAL blocks per segment")
	}

	w := &WAL{
		dir:              dir,
		blockSize:        blockSize,
		blocksPerSegment: blocksPerSegment,
		segmentSize:      blockSize * blocksPerSegment,
	}

	if err := w.initializeLastSegment(); err != nil {
		return nil, err
	}

	return w, nil
}

func (w *WAL) AppendPut(key string, value []byte) error {
	recordBytes, err := buildPutRecord(key, value)
	if err != nil {
		return err
	}
	return w.appendLogicalRecord(recordBytes)
}

func (w *WAL) AppendDelete(key string) error {
	recordBytes, err := buildDeleteRecord(key)
	if err != nil {
		return err
	}
	return w.appendLogicalRecord(recordBytes)
}

func (w *WAL) Close() error {
	if w.currentFile != nil {
		return w.currentFile.Close()
	}
	return nil
}

func (w *WAL) ReadAllRecords() ([]Record, error) {
	segments, err := w.listSegments()
	if err != nil {
		return nil, err
	}

	var records []Record
	var assembling []byte
	var assemblingCRC uint32
	var assemblingStarted bool

	for _, segPath := range segments {
		f, err := os.Open(segPath)
		if err != nil {
			return nil, err
		}

		stat, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return nil, err
		}

		var offset int64
		fileSize := stat.Size()

		for {
			frag, err := readNextFragment(f, &offset, fileSize, w.blockSize)
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = f.Close()
				return nil, fmt.Errorf("reading segment %s: %w", segPath, err)
			}

			switch frag.fragType {
			case FragFull:
				rec, err := parseLogicalRecord(frag.payload, frag.recordCRC)
				if err != nil {
					_ = f.Close()
					return nil, err
				}
				records = append(records, rec)

			case FragFirst:
				assembling = append([]byte{}, frag.payload...)
				assemblingCRC = frag.recordCRC
				assemblingStarted = true

			case FragMiddle:
				if !assemblingStarted {
					_ = f.Close()
					return nil, fmt.Errorf("middle fragment without first")
				}
				if frag.recordCRC != assemblingCRC {
					_ = f.Close()
					return nil, fmt.Errorf("fragment CRC mismatch")
				}
				assembling = append(assembling, frag.payload...)

			case FragLast:
				if !assemblingStarted {
					_ = f.Close()
					return nil, fmt.Errorf("last fragment without first")
				}
				if frag.recordCRC != assemblingCRC {
					_ = f.Close()
					return nil, fmt.Errorf("fragment CRC mismatch")
				}
				assembling = append(assembling, frag.payload...)

				rec, err := parseLogicalRecord(assembling, assemblingCRC)
				if err != nil {
					_ = f.Close()
					return nil, err
				}
				records = append(records, rec)

				assembling = nil
				assemblingCRC = 0
				assemblingStarted = false

			default:
				_ = f.Close()
				return nil, fmt.Errorf("unknown fragment type")
			}
		}

		_ = f.Close()
	}

	if assemblingStarted {
		return nil, fmt.Errorf("incomplete fragmented record at the end of WAL")
	}

	return records, nil
}

func buildPutRecord(key string, value []byte) ([]byte, error) {
	var buf bytes.Buffer

	if err := buf.WriteByte(OpPut); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(key))); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(value))); err != nil {
		return nil, err
	}
	if _, err := buf.Write([]byte(key)); err != nil {
		return nil, err
	}
	if _, err := buf.Write(value); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func buildDeleteRecord(key string) ([]byte, error) {
	var buf bytes.Buffer

	if err := buf.WriteByte(OpDelete); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(key))); err != nil {
		return nil, err
	}
	if _, err := buf.Write([]byte(key)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func parseLogicalRecord(recordBytes []byte, expectedCRC uint32) (Record, error) {
	calculated := crc32.ChecksumIEEE(recordBytes)
	if calculated != expectedCRC {
		return Record{}, fmt.Errorf(
			"logical record CRC mismatch: stored=%d calculated=%d",
			expectedCRC,
			calculated,
		)
	}

	r := bytes.NewReader(recordBytes)

	op, err := r.ReadByte()
	if err != nil {
		return Record{}, err
	}

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

		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyBytes); err != nil {
			return Record{}, err
		}

		valueBytes := make([]byte, valueLen)
		if _, err := io.ReadFull(r, valueBytes); err != nil {
			return Record{}, err
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

		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyBytes); err != nil {
			return Record{}, err
		}

		return Record{
			Op:  OpDelete,
			Key: string(keyBytes),
		}, nil

	default:
		return Record{}, fmt.Errorf("unknown operation type")
	}
}

func (w *WAL) appendLogicalRecord(recordBytes []byte) error {
	recordCRC := crc32.ChecksumIEEE(recordBytes)
	remaining := recordBytes
	first := true

	for len(remaining) > 0 {
		if w.currentOffset == int64(w.segmentSize) {
			if err := w.rotateSegment(); err != nil {
				return err
			}
		}

		blockOffset := int(w.currentOffset % int64(w.blockSize))
		remainingInBlock := w.blockSize - blockOffset

		if remainingInBlock < fragmentHeaderSize+1 {
			if err := w.padToEndOfBlock(remainingInBlock); err != nil {
				return err
			}
			continue
		}

		maxPayloadInThisBlock := remainingInBlock - fragmentHeaderSize
		chunkSize := min(len(remaining), maxPayloadInThisBlock)
		chunk := remaining[:chunkSize]

		var fragType byte
		last := chunkSize == len(remaining)

		switch {
		case first && last:
			fragType = FragFull
		case first:
			fragType = FragFirst
		case last:
			fragType = FragLast
		default:
			fragType = FragMiddle
		}

		if err := w.writeFragment(recordCRC, fragType, chunk); err != nil {
			return err
		}

		remaining = remaining[chunkSize:]
		first = false
	}

	return nil
}

func (w *WAL) writeFragment(recordCRC uint32, fragType byte, payload []byte) error {
	if err := binary.Write(w.currentFile, binary.LittleEndian, recordCRC); err != nil {
		return err
	}
	if _, err := w.currentFile.Write([]byte{fragType}); err != nil {
		return err
	}
	if err := binary.Write(w.currentFile, binary.LittleEndian, uint32(len(payload))); err != nil {
		return err
	}
	if _, err := w.currentFile.Write(payload); err != nil {
		return err
	}

	w.currentOffset += int64(fragmentHeaderSize + len(payload))
	return nil
}

func (w *WAL) padToEndOfBlock(n int) error {
	if n <= 0 {
		return nil
	}

	padding := make([]byte, n)
	if _, err := w.currentFile.Write(padding); err != nil {
		return err
	}

	w.currentOffset += int64(n)

	if w.currentOffset == int64(w.segmentSize) {
		return w.rotateSegment()
	}

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

	f, err := os.OpenFile(w.currentPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	w.currentFile = f
	w.currentOffset = 0
	return nil
}

func (w *WAL) initializeLastSegment() error {
	segments, err := w.listSegments()
	if err != nil {
		return err
	}

	if len(segments) == 0 {
		w.currentSegment = 1
		w.currentPath = w.segmentPath(w.currentSegment)

		f, err := os.OpenFile(w.currentPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return fmt.Errorf("can't create first WAL segment: %w", err)
		}

		w.currentFile = f
		w.currentOffset = 0
		return nil
	}

	lastPath := segments[len(segments)-1]
	lastSegmentNumber, err := extractSegmentNumber(lastPath)
	if err != nil {
		return err
	}

	stat, err := os.Stat(lastPath)
	if err != nil {
		return err
	}

	size := stat.Size()
	if size > int64(w.segmentSize) {
		return fmt.Errorf("segment %s exceeds configured segment size", lastPath)
	}

	if size == int64(w.segmentSize) {
		w.currentSegment = lastSegmentNumber + 1
		w.currentPath = w.segmentPath(w.currentSegment)

		f, err := os.OpenFile(w.currentPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return err
		}

		w.currentFile = f
		w.currentOffset = 0
		return nil
	}

	f, err := os.OpenFile(lastPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}

	w.currentSegment = lastSegmentNumber
	w.currentPath = lastPath
	w.currentFile = f
	w.currentOffset = size

	return nil
}

func readNextFragment(f *os.File, offset *int64, fileSize int64, blockSize int) (fragment, error) {
	for {
		if *offset >= fileSize {
			return fragment{}, io.EOF
		}

		blockOffset := int(*offset % int64(blockSize))
		remainingInBlock := blockSize - blockOffset

		if remainingInBlock < fragmentHeaderSize+1 {
			skip := int64(remainingInBlock)
			if _, err := f.Seek(skip, io.SeekCurrent); err != nil {
				return fragment{}, err
			}
			*offset += skip
			continue
		}

		if *offset+int64(fragmentHeaderSize) > fileSize {
			return fragment{}, io.EOF
		}

		var recordCRC uint32
		if err := binary.Read(f, binary.LittleEndian, &recordCRC); err != nil {
			return fragment{}, err
		}

		fragTypeBuf := make([]byte, 1)
		if _, err := io.ReadFull(f, fragTypeBuf); err != nil {
			return fragment{}, err
		}
		fragType := fragTypeBuf[0]

		var payloadLen uint32
		if err := binary.Read(f, binary.LittleEndian, &payloadLen); err != nil {
			return fragment{}, err
		}

		maxPayloadInThisBlock := remainingInBlock - fragmentHeaderSize
		if int(payloadLen) > maxPayloadInThisBlock {
			return fragment{}, fmt.Errorf("fragment payload exceeds block boundary")
		}

		if *offset+int64(fragmentHeaderSize)+int64(payloadLen) > fileSize {
			return fragment{}, io.EOF
		}

		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(f, payload); err != nil {
			return fragment{}, err
		}

		*offset += int64(fragmentHeaderSize) + int64(payloadLen)

		switch fragType {
		case FragFull, FragFirst, FragMiddle, FragLast:
			return fragment{
				recordCRC: recordCRC,
				fragType:  fragType,
				payload:   payload,
			}, nil
		default:
			return fragment{}, fmt.Errorf("invalid fragment type: %d", fragType)
		}
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
		ni, _ := extractSegmentNumber(segments[i])
		nj, _ := extractSegmentNumber(segments[j])
		return ni < nj
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
