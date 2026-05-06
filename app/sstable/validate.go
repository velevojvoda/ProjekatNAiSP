package sstable

import "bytes"

// ValidateMerkle ponovo izračuna Merkle stablo iz aktuelnog data.db i poredi sa
// onim što je snimljeno u merkle.db. Ako se neki list ne slaže, vraća tačno
// koji ključ je oštećen.
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
		if i >= len(mf.Leaves) || !bytes.Equal(mf.Leaves[i], leaf) {
			res.Valid = false
			res.MismatchedAt = append(res.MismatchedAt, i)
			if i < len(keys) {
				res.MismatchedKeys = append(res.MismatchedKeys, keys[i])
			}
		}
	}

	root := buildMerkleRootFromLeaves(leaves)
	if !bytes.Equal(root, mf.Root) {
		res.Valid = false
		res.RootMatch = false
	}

	return res, nil
}
