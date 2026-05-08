package wal

/*
Block and segment structure:
	Segments are files named wal_0001.log, wal_0002.log, etc.
	Each segment contains a fixed number of blocks (configurable).
	Each block is a fixed size (matching the BlockManager's block size).

Main functionality:
	NewWAL: Initializes the WAL, creating the directory if it doesn't exist, and determining where to append new records based on existing segments.
	AppendPut: Calls buildPutRecord to create a byte representation of the put record, then appends it as a logical record using appendLogicalRecord.
	AppendDelete: Similar to AppendPut, but for delete records.
	ReadAllRecords: Lists all segments, reads each block, and parses fragments to reconstruct logical records. Handles fragmentation across blocks and validates CRCs.
	DeleteFlushedSegments: Lists all segments and deletes those that are not currently being written to (i.e., not the current segment).

Interal logic:
	initializeLastSegment: On startup, determines the last segment and block to append to, ensuring that new records are appended correctly without overwriting existing data.
	appendLogicalRecord: Handles fragmentation of logical records into fragments that fit within blocks, writing them with appropriate headers and CRCs.
	writeFragment: Writes a single fragment to the current block, ensuring it does not exceed block boundaries.
	advanceToNextBlock: Moves to the next block within the current segment, or rotates to a new segment if the current one is full.
	rotateToNewSegment: Starts a new segment file when the current segment reaches its block limit.

Reading logic:
	parseFragmentsInBlock: Reads fragments from a block, reconstructing logical records based on fragment types (full, first, middle, last) and validating CRCs.
	parseLogicalRecord: Parses a complete logical record from its byte representation, validating the CRC and extracting the operation, key, and value.
	findEndOffsetInBlock: Scans through a block to find the offset where the next logical record can be appended, based on existing fragments.

Helper functions:
	listSegments: Lists all WAL segment files in the directory, sorted by segment number.
	segmentPath: Constructs the file path for a given segment number.
	extractSegmentNumber: Extracts the segment number from a segment file name.
	zeroCurrentBlock: Fills the current block buffer with zeros, used when starting a new block or segment.

*/

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

	"ProjekatNAiSP/app/block"
)

const (
	OpPut    byte = 1
	OpDelete byte = 2
)

// Fragment types for logical record fragmentation across blocks
const (
	FragFull   byte = 1
	FragFirst  byte = 2
	FragMiddle byte = 3
	FragLast   byte = 4
)

const fragmentHeaderSize = 9

type Record struct {
	Op    byte
	Key   string
	Value []byte
}

type WAL struct {
	dir              string
	blockSize        int
	blocksPerSegment int
	bm               *block.BlockManager

	currentSegment     int
	currentPath        string
	currentBlockNumber int64
	currentBlock       []byte
	blockOffset        int
}

func NewWAL(dir string, bm *block.BlockManager, blocksPerSegment int) (*WAL, error) {
	if bm == nil {
		return nil, fmt.Errorf("WAL requires a non-nil BlockManager")
	}
	if blocksPerSegment <= 0 {
		return nil, fmt.Errorf("invalid WAL blocks per segment")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	w := &WAL{
		dir:              dir,
		blockSize:        bm.BlockSize(),
		blocksPerSegment: blocksPerSegment,
		bm:               bm,
		currentBlock:     make([]byte, bm.BlockSize()),
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
		stat, err := os.Stat(segPath)
		if err != nil {
			return nil, err
		}
		size := stat.Size()
		if size == 0 {
			continue
		}
		if size%int64(w.blockSize) != 0 {
			return nil, fmt.Errorf(
				"segment %s size %d is not a multiple of block size %d "+
					"(possibly written with old non-block format — clear data/wal directory)",
				segPath, size, w.blockSize,
			)
		}
		numBlocks := size / int64(w.blockSize)

		for blockNum := int64(0); blockNum < numBlocks; blockNum++ {
			blockData, err := w.bm.ReadBlock(segPath, blockNum)
			if err != nil {
				return nil, fmt.Errorf("reading %s block %d: %w", segPath, blockNum, err)
			}

			if err := w.parseFragmentsInBlock(
				blockData, segPath, blockNum,
				&records, &assembling, &assemblingCRC, &assemblingStarted,
			); err != nil {
				return nil, err
			}
		}
	}

	if assemblingStarted {
		return nil, fmt.Errorf("incomplete fragmented record at end of WAL")
	}

	return records, nil
}

func (w *WAL) parseFragmentsInBlock(
	blockData []byte,
	segPath string,
	blockNum int64,
	records *[]Record,
	assembling *[]byte,
	assemblingCRC *uint32,
	assemblingStarted *bool,
) error {
	offset := 0
	for offset < w.blockSize {
		remainingInBlock := w.blockSize - offset
		if remainingInBlock < fragmentHeaderSize+1 {
			return nil
		}

		fragType := blockData[offset+4]
		if fragType == 0 {
			return nil
		}

		if fragType < FragFull || fragType > FragLast {
			return fmt.Errorf(
				"invalid fragment type %d in %s block %d at offset %d",
				fragType, segPath, blockNum, offset,
			)
		}

		recordCRC := binary.LittleEndian.Uint32(blockData[offset : offset+4])
		payloadLen := binary.LittleEndian.Uint32(blockData[offset+5 : offset+9])

		if int(payloadLen)+fragmentHeaderSize > remainingInBlock {
			return fmt.Errorf(
				"fragment payload (%d bytes) exceeds block boundary in %s block %d at offset %d",
				payloadLen, segPath, blockNum, offset,
			)
		}

		payloadStart := offset + fragmentHeaderSize
		payload := blockData[payloadStart : payloadStart+int(payloadLen)]
		offset += fragmentHeaderSize + int(payloadLen)

		switch fragType {
		case FragFull:
			rec, err := parseLogicalRecord(payload, recordCRC)
			if err != nil {
				return err
			}
			*records = append(*records, rec)

		case FragFirst:
			*assembling = append([]byte{}, payload...)
			*assemblingCRC = recordCRC
			*assemblingStarted = true

		case FragMiddle:
			if !*assemblingStarted {
				return fmt.Errorf("middle fragment without first")
			}
			if recordCRC != *assemblingCRC {
				return fmt.Errorf("fragment CRC mismatch")
			}
			*assembling = append(*assembling, payload...)

		case FragLast:
			if !*assemblingStarted {
				return fmt.Errorf("last fragment without first")
			}
			if recordCRC != *assemblingCRC {
				return fmt.Errorf("fragment CRC mismatch")
			}
			*assembling = append(*assembling, payload...)

			rec, err := parseLogicalRecord(*assembling, *assemblingCRC)
			if err != nil {
				return err
			}
			*records = append(*records, rec)

			*assembling = nil
			*assemblingCRC = 0
			*assemblingStarted = false
		}
	}
	return nil
}

func (w *WAL) appendLogicalRecord(recordBytes []byte) error {
	recordCRC := crc32.ChecksumIEEE(recordBytes)
	remaining := recordBytes
	first := true

	for len(remaining) > 0 {
		remainingInBlock := w.blockSize - w.blockOffset

		if remainingInBlock < fragmentHeaderSize+1 {
			if err := w.advanceToNextBlock(); err != nil {
				return err
			}
			continue
		}

		maxPayloadInBlock := remainingInBlock - fragmentHeaderSize
		chunkSize := min(len(remaining), maxPayloadInBlock)
		chunk := remaining[:chunkSize]

		last := chunkSize == len(remaining)
		var fragType byte
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
	totalLen := fragmentHeaderSize + len(payload)
	if w.blockOffset+totalLen > w.blockSize {
		return fmt.Errorf("fragment write would exceed block boundary")
	}

	binary.LittleEndian.PutUint32(w.currentBlock[w.blockOffset:w.blockOffset+4], recordCRC)
	w.currentBlock[w.blockOffset+4] = fragType
	binary.LittleEndian.PutUint32(w.currentBlock[w.blockOffset+5:w.blockOffset+9], uint32(len(payload)))
	copy(w.currentBlock[w.blockOffset+9:], payload)

	w.blockOffset += totalLen

	return w.bm.WriteBlock(w.currentPath, w.currentBlockNumber, w.currentBlock)
}

func (w *WAL) advanceToNextBlock() error {
	w.currentBlockNumber++
	w.blockOffset = 0

	if w.currentBlockNumber >= int64(w.blocksPerSegment) {
		return w.rotateToNewSegment()
	}

	w.zeroCurrentBlock()
	return nil
}

func (w *WAL) rotateToNewSegment() error {
	w.currentSegment++
	w.currentPath = w.segmentPath(w.currentSegment)
	w.currentBlockNumber = 0
	w.blockOffset = 0
	w.zeroCurrentBlock()
	return nil
}

func (w *WAL) zeroCurrentBlock() {
	for i := range w.currentBlock {
		w.currentBlock[i] = 0
	}
}

func (w *WAL) initializeLastSegment() error {
	segments, err := w.listSegments()
	if err != nil {
		return err
	}

	// If no segments exist, start with segment 1
	if len(segments) == 0 {
		w.currentSegment = 1
		w.currentPath = w.segmentPath(w.currentSegment)
		w.currentBlockNumber = 0
		w.blockOffset = 0
		return nil
	}

	// Start with the last segment and determine where to append next
	lastPath := segments[len(segments)-1]
	lastSegmentNumber, err := extractSegmentNumber(lastPath)
	if err != nil {
		return err
	}

	// Check the size of the last segment to determine if we can append there or need a new segment
	stat, err := os.Stat(lastPath)
	if err != nil {
		return err
	}
	size := stat.Size()

	// If the last segment is empty, we can start there
	if size == 0 {
		w.currentSegment = lastSegmentNumber
		w.currentPath = lastPath
		w.currentBlockNumber = 0
		w.blockOffset = 0
		return nil
	}

	// If the last segment has data, we need to find the end of the last logical record
	if size%int64(w.blockSize) != 0 {
		return fmt.Errorf(
			"segment %s size %d is not a multiple of block size %d "+
				"(possibly written with old non-block format — clear data/wal directory)",
			lastPath, size, w.blockSize,
		)
	}

	// Calculate how many blocks are in the last segment
	numBlocks := size / int64(w.blockSize)
	if numBlocks > int64(w.blocksPerSegment) {
		return fmt.Errorf(
			"segment %s has %d blocks, more than configured limit %d",
			lastPath, numBlocks, w.blocksPerSegment,
		)
	}

	// If the last segment is full, we need to start a new segment
	if numBlocks == int64(w.blocksPerSegment) {
		w.currentSegment = lastSegmentNumber + 1
		w.currentPath = w.segmentPath(w.currentSegment)
		w.currentBlockNumber = 0
		w.blockOffset = 0
		return nil
	}

	// Otherwise, we can start appending to the last segment, but we need to find the end of the last logical record
	w.currentSegment = lastSegmentNumber
	w.currentPath = lastPath
	w.currentBlockNumber = numBlocks - 1

	blockData, err := w.bm.ReadBlock(w.currentPath, w.currentBlockNumber)
	if err != nil {
		return fmt.Errorf("reading last block of %s: %w", w.currentPath, err)
	}
	copy(w.currentBlock, blockData)

	endOffset, err := findEndOffsetInBlock(w.currentBlock, w.blockSize)
	if err != nil {
		return err
	}
	w.blockOffset = endOffset

	if w.blockOffset == w.blockSize {
		if err := w.advanceToNextBlock(); err != nil {
			return err
		}
	}

	return nil
}

// Scans through the block data to find the offset where the next logical record can be appended.
func findEndOffsetInBlock(blockData []byte, blockSize int) (int, error) {
	offset := 0
	for offset < blockSize {
		remainingInBlock := blockSize - offset
		if remainingInBlock < fragmentHeaderSize+1 {
			return offset, nil
		}

		fragType := blockData[offset+4]
		if fragType == 0 {
			return offset, nil
		}

		if fragType < FragFull || fragType > FragLast {
			return 0, fmt.Errorf("invalid fragment type %d at offset %d", fragType, offset)
		}

		payloadLen := binary.LittleEndian.Uint32(blockData[offset+5 : offset+9])
		if int(payloadLen)+fragmentHeaderSize > remainingInBlock {
			return 0, fmt.Errorf(
				"fragment payload exceeds block boundary at offset %d", offset,
			)
		}

		offset += fragmentHeaderSize + int(payloadLen)
	}
	return offset, nil
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
			expectedCRC, calculated,
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

// Helper function to get path for a given segment number
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

func (w *WAL) DeleteFlushedSegments() error {
	segments, err := w.listSegments()
	if err != nil {
		return err
	}

	for _, segPath := range segments {
		if segPath == w.currentPath {
			continue
		}
		if err := os.Remove(segPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("Failed to delete WAL segment %s: %w", segPath, err)
		}
	}

	return nil
}
