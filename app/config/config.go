package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type Config struct {
	DataDir            string `json:"data_dir"`
	WALDir             string `json:"wal_dir"`
	MemtableMaxEntries int    `json:"memtable_max_entries"`
	BlockSizeKB        int    `json:"block_size_kb"`
	CacheCapacity      int    `json:"cache_capacity"`
	MemtableImpl       string `json:"memtable_impl"`
	MaxLSMLevels       int    `json:"max_lsm_levels"`
	MemtableCount      int    `json:"memtable_count"`
	MemtableSizeType   string `json:"memtable_size_type"`
	MemtableMaxSizeKB  int    `json:"memtable_max_size_kb"`
}

func LoadConfig(path string) (*Config, error) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("Can't read config file: %w", err)
	}

	var cfg Config
	if err := json.Unmarshal(bytes, &cfg); err != nil {
		return nil, fmt.Errorf("Can't parse JSON: %w", err)
	}

	applyDefaults(&cfg)
	createDirectories(&cfg)

	return &cfg, nil
}

func applyDefaults(cfg *Config) {
	if cfg.DataDir == "" {
		cfg.DataDir = "data"
	}
	if cfg.WALDir == "" {
		cfg.WALDir = filepath.Join(cfg.DataDir, "wal")
	}
	if cfg.MemtableMaxEntries <= 0 {
		cfg.MemtableMaxEntries = 1000
	}
	if cfg.BlockSizeKB != 4 && cfg.BlockSizeKB != 8 && cfg.BlockSizeKB != 16 {
		cfg.BlockSizeKB = 4
	}
	if cfg.CacheCapacity <= 0 {
		cfg.CacheCapacity = 1000
	}
	if cfg.MemtableImpl == "" {
		cfg.MemtableImpl = "hashmap"
	}
	if cfg.MaxLSMLevels <= 0 {
		cfg.MaxLSMLevels = 3
	}
	if cfg.MemtableCount <= 0 {
		cfg.MemtableCount = 1
	}
	if cfg.MemtableSizeType == "" {
		cfg.MemtableSizeType = "entries"
	}
	if cfg.MemtableSizeType != "entries" && cfg.MemtableSizeType != "kb" {
		cfg.MemtableSizeType = "entries"
	}
	if cfg.MemtableMaxSizeKB <= 0 {
		cfg.MemtableMaxSizeKB = 64
	}
}

func createDirectories(cfg *Config) {
	_ = os.MkdirAll(cfg.DataDir, 0o755)
	_ = os.MkdirAll(cfg.WALDir, 0o755)
}
