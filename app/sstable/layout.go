package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func normalizeOptions(opts BuildOptions) BuildOptions {
	if opts.BlockSize <= 0 {
		opts.BlockSize = 4096
	}
	if opts.SummaryStep <= 0 {
		opts.SummaryStep = 4
	}
	if opts.BloomM == 0 {
		opts.BloomM = 2048
	}
	if opts.BloomK == 0 {
		opts.BloomK = 3
	}
	if opts.TableID == "" {
		opts.TableID = fmt.Sprintf("table-%d", time.Now().UnixNano())
	}
	return opts
}

func prepareTable(opts BuildOptions) (*Table, error) {
	opts = normalizeOptions(opts)
	root := filepath.Join(opts.Dir, opts.TableID)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &Table{
		ID:          opts.TableID,
		Dir:         root,
		DataPath:    filepath.Join(root, "data.db"),
		IndexPath:   filepath.Join(root, "index.db"),
		SummaryPath: filepath.Join(root, "summary.db"),
		FilterPath:  filepath.Join(root, "filter.db"),
		MerklePath:  filepath.Join(root, "merkle.db"),
		BlockSize:   opts.BlockSize,
	}, nil
}

func Open(dir string, blockSize int) (*Table, error) {
	if blockSize <= 0 {
		blockSize = 4096
	}
	return &Table{
		ID:          filepath.Base(dir),
		Dir:         dir,
		DataPath:    filepath.Join(dir, "data.db"),
		IndexPath:   filepath.Join(dir, "index.db"),
		SummaryPath: filepath.Join(dir, "summary.db"),
		FilterPath:  filepath.Join(dir, "filter.db"),
		MerklePath:  filepath.Join(dir, "merkle.db"),
		BlockSize:   blockSize,
	}, nil
}
