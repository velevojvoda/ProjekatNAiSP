package sstable

import (
	"crypto/sha256"
	"encoding/binary"
	"io"
	"os"
)

// merkleFile je ono što fizički čuvamo u merkle.db.
// Format na disku (binarno, bez JSON-a — u skladu sa pravilima polaganja):
//
//	[RootLen: 4B][Root bytes]
//	[NumLeaves: 4B]
//	za svaki list: [LeafLen: 4B][Leaf bytes]
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

func writeMerkleFile(path string, values [][]byte) error {
	leaves := buildMerkleLeaves(values)
	root := buildMerkleRootFromLeaves(leaves)

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// header: [RootLen][Root][NumLeaves]
	header := make([]byte, 4+len(root)+4)
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(root)))
	copy(header[4:4+len(root)], root)
	binary.LittleEndian.PutUint32(header[4+len(root):], uint32(len(leaves)))
	if _, err := f.Write(header); err != nil {
		return err
	}

	// listovi, jedan po jedan
	for _, leaf := range leaves {
		buf := make([]byte, 4+len(leaf))
		binary.LittleEndian.PutUint32(buf[0:4], uint32(len(leaf)))
		copy(buf[4:], leaf)
		if _, err := f.Write(buf); err != nil {
			return err
		}
	}
	return nil
}

func readMerkleFile(path string) (merkleFile, error) {
	var mf merkleFile

	f, err := os.Open(path)
	if err != nil {
		return mf, err
	}
	defer f.Close()

	var lenBuf [4]byte

	if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
		return mf, err
	}
	rootLen := binary.LittleEndian.Uint32(lenBuf[:])
	root := make([]byte, rootLen)
	if _, err := io.ReadFull(f, root); err != nil {
		return mf, err
	}
	mf.Root = root

	if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
		return mf, err
	}
	numLeaves := binary.LittleEndian.Uint32(lenBuf[:])

	mf.Leaves = make([][]byte, 0, numLeaves)
	for i := uint32(0); i < numLeaves; i++ {
		if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
			return mf, err
		}
		leafLen := binary.LittleEndian.Uint32(lenBuf[:])
		leaf := make([]byte, leafLen)
		if _, err := io.ReadFull(f, leaf); err != nil {
			return mf, err
		}
		mf.Leaves = append(mf.Leaves, leaf)
	}

	return mf, nil
}
