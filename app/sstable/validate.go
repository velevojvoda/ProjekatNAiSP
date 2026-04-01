package sstable

import "encoding/hex"

func (t *Table) ValidateMerkle() (ValidationResult, error) {
	mf, err := readMerkleFile(t.MerklePath)
	if err != nil {
		return ValidationResult{}, err
	}

	records, err := t.AllRecords()
	if err != nil {
		return ValidationResult{}, err
	}

	values := make([][]byte, 0, len(records))
	keys := make([]string, 0, len(records))
	for _, rec := range records {
		values = append(values, rec.Value)
		keys = append(keys, rec.Key)
	}

	leaves := buildMerkleLeaves(values)
	res := ValidationResult{Valid: true, RootMatch: true}

	for i, leaf := range leaves {
		leafHex := hex.EncodeToString(leaf)
		if i >= len(mf.Leaves) || mf.Leaves[i] != leafHex {
			res.Valid = false
			res.MismatchedAt = append(res.MismatchedAt, i)
			if i < len(keys) {
				res.MismatchedKeys = append(res.MismatchedKeys, keys[i])
			}
		}
	}

	rootHex := hex.EncodeToString(buildMerkleRootFromLeaves(leaves))
	if rootHex != mf.Root {
		res.Valid = false
		res.RootMatch = false
	}

	return res, nil
}
