package block

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

const kilobyte = 1024

var (
	ErrInvalidBlockNumber = errors.New("block number must be greater than or equal to 0")
	ErrInvalidBlockData   = errors.New("block data size must match block size")
	ErrInvalidBlockSize   = errors.New("block size must be 4KB, 8KB, or 16KB")
	ErrPartialBlockRead   = errors.New("could not read a full block from disk")
	ErrShortBlockWrite    = errors.New("could not write a full block to disk")
)

type BlockManager struct {
	blockSize int
	cache     *BlockCache
}

func NewBlockManager(blockSizeKB int, cacheCapacity int) (*BlockManager, error) {
	blockSizeBytes, err := normalizeBlockSize(blockSizeKB)
	if err != nil {
		return nil, err
	}

	cache, err := NewBlockCache(cacheCapacity)
	if err != nil {
		return nil, err
	}

	return &BlockManager{
		blockSize: blockSizeBytes,
		cache:     cache,
	}, nil
}

func (m *BlockManager) ReadBlock(path string, blockNumber int64) ([]byte, error) {
	if blockNumber < 0 {
		return nil, ErrInvalidBlockNumber
	}

	id := BlockID{
		FilePath:    path,
		BlockNumber: blockNumber,
	}

	if data, found := m.cache.Get(id); found {
		return data, nil
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	buffer := make([]byte, m.blockSize)
	offset := m.offsetFor(blockNumber)

	n, err := file.ReadAt(buffer, offset)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, ErrPartialBlockRead
		}
		return nil, err
	}

	if n != m.blockSize {
		return nil, ErrPartialBlockRead
	}

	m.cache.Put(id, buffer)
	return cloneBytes(buffer), nil
}

func (m *BlockManager) WriteBlock(path string, blockNumber int64, data []byte) error {
	if blockNumber < 0 {
		return ErrInvalidBlockNumber
	}

	if len(data) != m.blockSize {
		return ErrInvalidBlockData
	}

	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	offset := m.offsetFor(blockNumber)

	n, err := file.WriteAt(data, offset)
	if err != nil {
		return err
	}

	if n != m.blockSize {
		return ErrShortBlockWrite
	}

	id := BlockID{
		FilePath:    path,
		BlockNumber: blockNumber,
	}

	m.cache.UpdateIfPresent(id, data)
	return nil
}

func (m *BlockManager) BlockSize() int {
	return m.blockSize
}

func normalizeBlockSize(blockSizeKB int) (int, error) {
	switch blockSizeKB {
	case 4, 8, 16:
		return blockSizeKB * kilobyte, nil
	default:
		return 0, ErrInvalidBlockSize
	}
}

func (m *BlockManager) offsetFor(blockNumber int64) int64 {
	return blockNumber * int64(m.blockSize)
}
