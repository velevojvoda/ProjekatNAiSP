package sstable

import (
	"sort"

	"ProjekatNAiSP/app/model"
)

func Build(records []model.Record, opts BuildOptions) (*Table, error) {
	table, err := prepareTable(opts)
	if err != nil {
		return nil, err
	}

	sorted := append([]model.Record(nil), records...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Key < sorted[j].Key })

	indexEntries, values, err := writeDataFile(table.DataPath, sorted)
	if err != nil {
		return nil, err
	}
	allSummaryEntries, err := writeIndexFile(table.IndexPath, indexEntries)
	if err != nil {
		return nil, err
	}
	if _, err := writeSummaryFile(table.SummaryPath, allSummaryEntries, normalizeOptions(opts).SummaryStep); err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(sorted))
	for _, rec := range sorted {
		keys = append(keys, rec.Key)
	}

	norm := normalizeOptions(opts)
	if err := writeBloomFilter(table.FilterPath, keys, norm.BloomM, norm.BloomK); err != nil {
		return nil, err
	}
	if err := writeMerkleFile(table.MerklePath, values); err != nil {
		return nil, err
	}
	return table, nil
}
