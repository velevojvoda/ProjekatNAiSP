package sstable

import (
	"encoding/binary"
	"hash/fnv"
	"os"
)

type BloomFilter struct {
	M    uint64
	K    uint32
	Bits []byte
}

func newBloomFilter(m uint64, k uint32) *BloomFilter {
	byteLen := (m + 7) / 8
	return &BloomFilter{M: m, K: k, Bits: make([]byte, byteLen)}
}

func (bf *BloomFilter) Add(key string) {
	for i := uint32(0); i < bf.K; i++ {
		pos := bf.hash(key, i) % bf.M
		bf.Bits[pos/8] |= 1 << (pos % 8)
	}
}

func (bf *BloomFilter) MightContain(key string) bool {
	for i := uint32(0); i < bf.K; i++ {
		pos := bf.hash(key, i) % bf.M
		if bf.Bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) hash(key string, seed uint32) uint64 {
	h := fnv.New64a()
	var seedBuf [4]byte
	binary.LittleEndian.PutUint32(seedBuf[:], seed)
	_, _ = h.Write(seedBuf[:])
	_, _ = h.Write([]byte(key))
	return h.Sum64()
}

func writeBloomFilter(path string, keys []string, m uint64, k uint32) error {
	bf := newBloomFilter(m, k)
	for _, key := range keys {
		bf.Add(key)
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	head := make([]byte, 12)
	binary.LittleEndian.PutUint64(head[0:8], bf.M)
	binary.LittleEndian.PutUint32(head[8:12], bf.K)
	if _, err := f.Write(head); err != nil {
		return err
	}
	_, err = f.Write(bf.Bits)
	return err
}

func readBloomFilter(path string) (*BloomFilter, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(buf) < 12 {
		return nil, ErrCorruptedData
	}

	m := binary.LittleEndian.Uint64(buf[0:8])
	k := binary.LittleEndian.Uint32(buf[8:12])
	bits := append([]byte(nil), buf[12:]...)
	return &BloomFilter{M: m, K: k, Bits: bits}, nil
}
