package sstable

import (
	"crypto/sha256"
	"encoding/binary"
	"io"

	"ProjekatNAiSP/app/block"
)

type merkleFile struct {
	Root   []byte
	Leaves [][]byte
}

func hashBytes(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}

func buildMerkleLeaves(values [][]byte) [][]byte {
	leaves := make([][]byte, 0, len(values))
	for _, v := range values {
		leaves = append(leaves, hashBytes(v))
	}
	return leaves
}

func buildMerkleRootFromLeaves(leaves [][]byte) []byte {
	if len(leaves) == 0 {
		empty := sha256.Sum256(nil)
		return empty[:]
	}

	level := make([][]byte, len(leaves))
	copy(level, leaves)
	for len(level) > 1 {
		next := make([][]byte, 0, (len(level)+1)/2)
		for i := 0; i < len(level); i += 2 {
			left := level[i]
			right := left
			if i+1 < len(level) {
				right = level[i+1]
			}
			combined := make([]byte, 0, len(left)+len(right))
			combined = append(combined, left...)
			combined = append(combined, right...)
			next = append(next, hashBytes(combined))
		}
		level = next
	}
	return level[0]
}

func writeMerkleFile(bm *block.BlockManager, path string, blockSize int, values [][]byte) error {
	leaves := buildMerkleLeaves(values)
	root := buildMerkleRootFromLeaves(leaves)

	var buf []byte

	// header: [RootLen][Root][NumLeaves]
	header := make([]byte, 4+len(root)+4)
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(root)))
	copy(header[4:4+len(root)], root)
	binary.LittleEndian.PutUint32(header[4+len(root):], uint32(len(leaves)))
	buf = append(buf, header...)

	for _, leaf := range leaves {
		lb := make([]byte, 4+len(leaf))
		binary.LittleEndian.PutUint32(lb[0:4], uint32(len(leaf)))
		copy(lb[4:], leaf)
		buf = append(buf, lb...)
	}

	return writeAllBytes(bm, path, blockSize, buf)
}

func readMerkleFile(bm *block.BlockManager, path string, blockSize int) (merkleFile, error) {
	var mf merkleFile
	var pos int64

	lenBuf, err := readBytesAt(bm, path, blockSize, pos, 4)
	if err != nil {
		return mf, err
	}
	pos += 4
	rootLen := int(binary.LittleEndian.Uint32(lenBuf))

	root, err := readBytesAt(bm, path, blockSize, pos, rootLen)
	if err != nil {
		return mf, err
	}
	pos += int64(rootLen)
	mf.Root = root

	lenBuf, err = readBytesAt(bm, path, blockSize, pos, 4)
	if err != nil {
		return mf, err
	}
	pos += 4
	numLeaves := int(binary.LittleEndian.Uint32(lenBuf))

	mf.Leaves = make([][]byte, 0, numLeaves)
	for i := 0; i < numLeaves; i++ {
		lb, err := readBytesAt(bm, path, blockSize, pos, 4)
		if err != nil {
			return mf, err
		}
		pos += 4
		leafLen := int(binary.LittleEndian.Uint32(lb))

		leaf, err := readBytesAt(bm, path, blockSize, pos, leafLen)
		if err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return mf, err
		}
		pos += int64(leafLen)
		mf.Leaves = append(mf.Leaves, leaf)
	}

	return mf, nil
}
