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

// fragment types, in case a logical record is split across multiple blocks
const (
	FragFull   byte = 1
	FragFirst  byte = 2
	FragMiddle byte = 3
	FragLast   byte = 4
)

// [recordCRC][fragType][payloadLen][payload]
const fragmentHeaderSize = 7 // 4 bytes CRC + 1 byte frag type + 2 bytes payload length

// record format:
// [Op][keyLen][valueLen][keyBytes][valueBytes] for put
// [Op][keyLen][keyBytes] for delete
type Record struct {
	Op    byte
	Key   string
	Value []byte
}

// fragment format:
// [recordCRC][fragType][payloadLen][payload]
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
	currentOffset  int64 // offset within current segment
}

func NewWAL(dir string, blockSize int, blocksPerSegment int) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	if blockSize <= 0 {
		return nil, fmt.Errorf("Invalid WAL block size")
	}

	if blocksPerSegment <= 0 {
		return nil, fmt.Errorf("Invalid WAL blocks per segment")
	}

	w := &WAL{
		dir:              dir,
		blockSize:        blockSize,
		blocksPerSegment: blocksPerSegment,
		segmentSize:      blockSize * blocksPerSegment,
	}

	if err := w.initializeLastSegment(); err != nil {
		return nil, err
	} // if there are existing segments, we will append to the last one; otherwise we create a new one

	return w, nil
}

func (w *WAL) AppendPut(key string, value []byte) error {
	recordBytes, err := buildPutRecord(key, value) // build the logical record in memory
	if err != nil {
		return err
	}
	return w.appendLogicalRecord(recordBytes) // append the logical record to WAL, splitting into fragments if needed
}

func (w *WAL) AppendDelete(key string) error {
	recordBytes, err := buildDeleteRecord(key) //same logic as AppendPut, but for delete operation
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

// for recovery
func (w *WAL) ReadAllRecords() ([]Record, error) {
	segments, err := w.listSegments() //wal_0001.log, wal_0002.log, ...
	if err != nil {
		return nil, err
	}

	var records []Record
	var assembling []byte     // all collected fragments of the currently assembling logical record
	var assemblingCRC uint32  // CRC of that record
	var asseblingStrated bool // if we are in the middle of assembling a fragmented record

	for _, segPath := range segments {
		f, err := os.Open(segPath)
		if err != nil {
			return nil, err
		}
		stat, err := f.Stat() // get file size for boundary checks
		if err != nil {
			_ = f.Close()
			return nil, err
		}

		var offset int64 = 0
		filesize := stat.Size()

		for {
			frag, err := readNextFragment(f, &offset, filesize, w.blockSize) //read next fragment one by one
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = f.Close()
				return nil, fmt.Errorf("reading segment %s: %w", segPath, err)
			}

			switch frag.fragType { // handle fragment based on its type
			case FragFull: // if it's a full fragment, we can parse it directly into a logical record
				rec, err := parseLogicalRecord(frag.payload, frag.recordCRC)
				if err != nil {
					_ = f.Close()
					return nil, err
				}
				records = append(records, rec)
			case FragFirst: // if it's the first fragment of a multi-fragment record, we start assembling
				assembling = append([]byte{}, frag.payload...)
				assemblingCRC = frag.recordCRC
				asseblingStrated = true

			case FragMiddle: // if it's a middle fragment, we check that it belongs to the same logical record and append its payload
				if !asseblingStrated {
					_ = f.Close()
					return nil, fmt.Errorf("Middle fragment without first")
				}
				if frag.recordCRC != assemblingCRC {
					_ = f.Close()
					return nil, fmt.Errorf("fragment CRC mismatch")
				}
				assembling = append(assembling, frag.payload...)
			case FragLast: // if it's the last fragment, we check that it belongs to the same logical record, append its payload, and then parse the full logical record
				if !asseblingStrated {
					_ = f.Close()
					return nil, fmt.Errorf("Last fragment without first")
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
				asseblingStrated = false
			default:
				_ = f.Close()
				return nil, fmt.Errorf("unknown fragment type")
			}
		}
		_ = f.Close()
	}

	if asseblingStrated { // if we finished reading all segments but assebling is still happening
		return nil, fmt.Errorf("incomplete fragmented record at the end of WAL")
	}
	return records, nil
}

func buildPutRecord(key string, value []byte) ([]byte, error) { // build the logical record for a put operation in memory
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

func buildDeleteRecord(key string) ([]byte, error) { // same logic as buildPutRecord
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

func parseLogicalRecord(recordBytes []byte, expectedCRC uint32) (Record, error) { // parse the logical record from the assembled bytes, checking CRC and format
	calculated := crc32.ChecksumIEEE(recordBytes)
	if calculated != expectedCRC {
		return Record{}, fmt.Errorf("logical record CRC mismatch: stored=%d calculated=%d", expectedCRC, calculated)
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
			return Record{}, nil
		}

		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyBytes); err != nil {
			return Record{}, nil
		}

		return Record{
			Op:  OpDelete,
			Key: string(keyBytes),
		}, nil
	default:
		return Record{}, fmt.Errorf("unkown operation type")
	}
}

func (w *WAL) appendLogicalRecord(recordBytes []byte) error { // append the logical record to WAL, splitting into fragments if needed
	recordCRC := crc32.ChecksumIEEE(recordBytes)
	remaining := recordBytes
	first := true

	for len(remaining) > 0 { // goes fragment by fragment
		if w.currentOffset == int64(w.segmentSize) { // open new segment if current one is full
			if err := w.rotateSegment(); err != nil {
				return err
			}
		}

		blockOffset := int(w.currentOffset % int64(w.blockSize)) // current offset within the block
		remainingInBlock := w.blockSize - blockOffset

		if remainingInBlock < fragmentHeaderSize+1 { // if there is not enough space for at least 1 byte of payload and header, we pad the rest of the block with zeros and move to the next block
			if err := w.padToEndOfBlock(remainingInBlock); err != nil {
				return err
			}
			continue
		}

		maxPayloadInThisBlock := remainingInBlock - fragmentHeaderSize // maximum payload we can write in this block after accounting for header
		chunkSize := min(len(remaining), maxPayloadInThisBlock)        // how much of the remaining logical record we will put in this fragment
		chunk := remaining[:chunkSize]                                 // the actual payload for this fragment

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

		remaining = remaining[chunkSize:] // update the remaining part of the logical record that we still need to write
		first = false
	}

	return nil
}

func (w *WAL) writeFragment(recordCRC uint32, fragType byte, payload []byte) error { // write a single fragment to the current WAL segment at the current offset
	if err := binary.Write(w.currentFile, binary.LittleEndian, recordCRC); err != nil {
		return nil
	}
	if _, err := w.currentFile.Write([]byte{fragType}); err != nil {
		return nil
	}
	if err := binary.Write(w.currentFile, binary.LittleEndian, uint32(len(payload))); err != nil {
		return nil
	}
	if _, err := w.currentFile.Write(payload); err != nil {
		return err
	}

	w.currentOffset += int64(fragmentHeaderSize + len(payload))
	return nil
}

func (w *WAL) padToEndOfBlock(n int) error { // write n zero bytes to pad the rest of the block
	if n <= 0 {
		return nil
	}
	padding := make([]byte, n)
	if _, err := w.currentFile.Write(padding); err != nil {
		return err
	}
	if w.currentOffset == int64(w.segmentSize) { // if after padding we are at the end of the segment, rotate to a new segment
		return w.rotateSegment()
	}
	return nil
}

func (w *WAL) rotateSegment() error { // close current segment and open a new one with incremented segment number
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

func (w *WAL) initializeLastSegment() error { // when we create the WAL, we check if there are already existing segments
	segments, err := w.listSegments()
	if err != nil {
		return nil
	}
	if len(segments) == 0 {
		w.currentSegment = 1
		w.currentPath = w.segmentPath(w.currentSegment) // if there are no existing segments, we start with segment 1

		f, err := os.OpenFile(w.currentPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return fmt.Errorf("can't create first WAL segment: %w", err)
		}

		w.currentFile = f
		w.currentOffset = 0
		return nil
	}

	lastPath := segments[len(segments)-1]
	lastSegmentNumber, err := extractSegmentNumber(lastPath) // if there are existing segments, we will append to the last one; otherwise we create a new one
	if err != nil {
		return err
	}
	stat, err := os.Stat(lastPath)
	if err != nil {
		return nil
	}
	size := stat.Size()
	if size > int64(w.segmentSize) { // if the last segment is larger than the configured segment size, or corrupted
		return fmt.Errorf("segment %s esexceeds configured segment size", lastPath)
	}

	if size == int64(w.segmentSize) { // if the last segment is exactly full, we start a new segment
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
		if *offset >= fileSize { // if we reached the end of the file, return EOF
			return fragment{}, io.EOF
		}

		blockOffset := int(*offset % int64(blockSize)) // current offset within the block
		remainingInBlock := blockSize - blockOffset    // how many bytes are remaining in the current block

		if remainingInBlock < fragmentHeaderSize+1 {
			skip := int64(remainingInBlock)
			if _, err := f.Seek(skip, io.SeekCurrent); err != nil {
				return fragment{}, err
			}
			*offset += skip
			continue
		}

		if *offset+int64(fragmentHeaderSize) > fileSize { // if there is not enough bytes left in the file for even the fragment header, EOF
			return fragment{}, io.EOF
		}

		var recordCRC uint32
		if err := binary.Read(f, binary.LittleEndian, &recordCRC); err != nil { // read the fragment header
			return fragment{}, err
		}

		fragTypeBuf := make([]byte, 1)
		if _, err := io.ReadFull(f, fragTypeBuf); err != nil { // read the fragment type
			return fragment{}, err
		}
		fragType := fragTypeBuf[0]

		var payloadLen uint32
		if err := binary.Read(f, binary.LittleEndian, &payloadLen); err != nil { // read the payload length
			return fragment{}, err
		}

		maxPayloadInThisBlock := remainingInBlock - fragmentHeaderSize
		if int(payloadLen) > maxPayloadInThisBlock { // if the payload length exceeds the remaining space in the block, it means the fragment is corrupted
			return fragment{}, fmt.Errorf("fragment payload exceeds block boundary")
		}

		if *offset+int64(fragmentHeaderSize)+int64(payloadLen) > fileSize { // if there is not enough bytes left in the file for the whole fragment, EOF
			return fragment{}, io.EOF
		}

		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(f, payload); err != nil { // read the payload
			return fragment{}, err
		}

		*offset += int64(fragmentHeaderSize) + int64(payloadLen) // update the offset for the next read

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

func (w *WAL) listSegments() ([]string, error) { // list all WAL segment files in the WAL directory, sort them by segment number, and return their paths
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
		if strings.HasPrefix(name, "wal_") && strings.HasSuffix(name, ".log") { // check that the file name matches the expected pattern for WAL segments (wal_0001.log, wal_0002.log...)
			segments = append(segments, filepath.Join(w.dir, name))
		}
	}

	sort.Slice(segments, func(i, j int) bool { // sort the segments by their segment number extracted from the file name
		ni, _ := extractSegmentNumber(segments[i])
		nj, _ := extractSegmentNumber(segments[j])
		return ni < nj
	})

	return segments, nil
}

func (w *WAL) segmentPath(segment int) string { // construct the file path for a given segment number, e.g. wal_0001.log, wal_0002.log, etc.
	return filepath.Join(w.dir, fmt.Sprintf("wal_%04d.log", segment))
}

func extractSegmentNumber(path string) (int, error) { // extract the segment number from the segment file name, e.g. from wal_0001.log extract 1...
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
