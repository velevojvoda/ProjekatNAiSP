package sstable

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
)

type merkleFile struct {
	Root   string   `json:"root"`
	Leaves []string `json:"leaves"`
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
			combined := append(append([]byte(nil), left...), right...)
			next = append(next, hashBytes(combined))
		}
		level = next
	}
	return level[0]
}

func writeMerkleFile(path string, values [][]byte) error {
	leaves := buildMerkleLeaves(values)
	root := buildMerkleRootFromLeaves(leaves)
	mf := merkleFile{Root: hex.EncodeToString(root), Leaves: make([]string, 0, len(leaves))}
	for _, leaf := range leaves {
		mf.Leaves = append(mf.Leaves, hex.EncodeToString(leaf))
	}
	data, err := json.MarshalIndent(mf, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func readMerkleFile(path string) (merkleFile, error) {
	var mf merkleFile
	data, err := os.ReadFile(path)
	if err != nil {
		return mf, err
	}
	err = json.Unmarshal(data, &mf)
	return mf, err
}
