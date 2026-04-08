package memtable

import "ProjekatNAiSP/app/model"

type HashMapMemtable struct {
	data             map[string]model.Record
	maxEntries       int
	maxSizeBytes     int
	sizeType         string
	currentSizeBytes int
	count            int
}

func NewHashMapMemtable(maxEntries int, maxSizeKB int, sizeType string) *HashMapMemtable {
	return &HashMapMemtable{
		data:         make(map[string]model.Record),
		maxEntries:   maxEntries,
		maxSizeBytes: maxSizeKB * 1024,
		sizeType:     sizeType,
	}
}

func (m *HashMapMemtable) Put(key string, value []byte) error {
	newRecord := model.Record{
		Key:       key,
		Value:     value,
		Tombstone: false,
	}

	if oldRecord, exists := m.data[key]; exists {
		m.currentSizeBytes -= recordSize(oldRecord)
	} else {
		m.count++
	}

	m.data[key] = newRecord
	m.currentSizeBytes += recordSize(newRecord)

	return nil
}

func (m *HashMapMemtable) Get(key string) (*model.Record, bool) {
	record, ok := m.data[key]
	if !ok {
		return nil, false
	}

	recCopy := record
	return &recCopy, true
}

func (m *HashMapMemtable) Delete(key string) error {
	newRecord := model.Record{
		Key:       key,
		Value:     nil,
		Tombstone: true,
	}

	if oldRecord, exists := m.data[key]; exists {
		m.currentSizeBytes -= recordSize(oldRecord)
	} else {
		m.count++
	}

	m.data[key] = newRecord
	m.currentSizeBytes += recordSize(newRecord)

	return nil
}

func (m *HashMapMemtable) IsFull() bool {
	if m.sizeType == "kb" {
		return m.currentSizeBytes >= m.maxSizeBytes
	}
	return m.count >= m.maxEntries
}

func (m *HashMapMemtable) Records() []model.Record {
	records := make([]model.Record, 0, len(m.data))
	for _, record := range m.data {
		records = append(records, record)
	}
	return records
}

func recordSize(record model.Record) int {
	size := len(record.Key) + len(record.Value)
	size += 1 // tombstone
	return size
}
