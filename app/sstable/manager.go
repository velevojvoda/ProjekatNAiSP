package sstable

import (
	"os"
	"path/filepath"
	"sort"

	"ProjekatNAiSP/app/model"
)

type Manager struct {
	BaseDir string
	Options BuildOptions
}

func NewManager(baseDir string, opts BuildOptions) *Manager {
	opts.Dir = baseDir
	return &Manager{BaseDir: baseDir, Options: opts}
}

func (m *Manager) BuildFromRecords(records []model.Record) (*Table, error) {
	opts := m.Options
	opts.Dir = m.BaseDir
	return Build(records, opts)
}

func (m *Manager) Open(tableDir string) (*Table, error) {
	return Open(tableDir, m.Options.BlockSize)
}

func (m *Manager) Get(table *Table, key string) (GetResult, error) {
	return table.Get(key)
}

func (m *Manager) Validate(table *Table) (ValidationResult, error) {
	return table.ValidateMerkle()
}

func (m *Manager) LoadExistingTables() ([]*Table, error) {
	entries, err := os.ReadDir(m.BaseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	tables := make([]*Table, 0)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		table, err := Open(filepath.Join(m.BaseDir, entry.Name()), m.Options.BlockSize)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	sort.Slice(tables, func(i, j int) bool {
		li, _ := os.Stat(tables[i].Dir)
		lj, _ := os.Stat(tables[j].Dir)
		if li == nil || lj == nil {
			return tables[i].Dir < tables[j].Dir
		}
		return li.ModTime().Before(lj.ModTime())
	})

	return tables, nil
}
