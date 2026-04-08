package memtable

import (
	"ProjekatNAiSP/app/model"
	"math/rand"
	"time"
)

const (
	defaultSkipListMaxLevel = 6
	defaultSkipListP        = 0.5
)

type skipListNode struct {
	key      string
	record   model.Record
	forwards []*skipListNode
}

type SkipListMemtable struct {
	header           *skipListNode
	level            int
	maxLevel         int
	p                float64
	maxEntries       int
	maxSizeBytes     int
	sizeType         string
	currentSizeBytes int
	count            int
	rng              *rand.Rand
}

func NewSkipListMemtable(maxEntries int, maxSizeKB int, sizeType string) *SkipListMemtable {
	header := &skipListNode{
		forwards: make([]*skipListNode, defaultSkipListMaxLevel),
	}

	return &SkipListMemtable{
		header:       header,
		level:        1,
		maxLevel:     defaultSkipListMaxLevel,
		p:            defaultSkipListP,
		maxEntries:   maxEntries,
		maxSizeBytes: maxSizeKB * 1024,
		sizeType:     sizeType,
		rng:          rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *SkipListMemtable) randomLevel() int {
	level := 1
	for level < s.maxLevel && s.rng.Float64() < s.p {
		level++
	}
	return level
}

func (s *SkipListMemtable) findUpdateNodes(key string) []*skipListNode {
	update := make([]*skipListNode, s.maxLevel)
	current := s.header

	for i := s.level - 1; i >= 0; i-- {
		for current.forwards[i] != nil && current.forwards[i].key < key {
			current = current.forwards[i]
		}
		update[i] = current
	}

	return update
}

func (s *SkipListMemtable) Get(key string) (*model.Record, bool) {
	current := s.header
	for i := s.level - 1; i >= 0; i-- {
		for current.forwards[i] != nil && current.forwards[i].key < key {
			current = current.forwards[i]
		}
	}

	current = current.forwards[0]
	if current != nil && current.key == key {
		recCopy := current.record
		return &recCopy, true
	}

	return nil, false
}

func (s *SkipListMemtable) Put(key string, value []byte) error {
	record := model.Record{
		Key:       key,
		Value:     value,
		Tombstone: false,
	}
	return s.upsert(record)
}

func (s *SkipListMemtable) Delete(key string) error {
	record := model.Record{
		Key:       key,
		Value:     nil,
		Tombstone: true,
	}
	return s.upsert(record)
}

func (s *SkipListMemtable) upsert(record model.Record) error {
	update := s.findUpdateNodes(record.Key)
	current := update[0].forwards[0]

	if current != nil && current.key == record.Key {
		s.currentSizeBytes -= recordSize(current.record)
		current.record = record
		s.currentSizeBytes += recordSize(record)
		return nil
	}

	newLevel := s.randomLevel()
	if newLevel > s.level {
		for i := s.level; i < newLevel; i++ {
			update[i] = s.header
		}
		s.level = newLevel
	}

	newNode := &skipListNode{
		key:      record.Key,
		record:   record,
		forwards: make([]*skipListNode, newLevel),
	}

	for i := 0; i < newLevel; i++ {
		newNode.forwards[i] = update[i].forwards[i]
		update[i].forwards[i] = newNode
	}

	s.count++
	s.currentSizeBytes += recordSize(record)

	return nil
}

func (s *SkipListMemtable) IsFull() bool {
	if s.sizeType == "kb" {
		return s.currentSizeBytes >= s.maxSizeBytes
	}
	return s.count >= s.maxEntries
}

func (s *SkipListMemtable) Records() []model.Record {
	records := make([]model.Record, 0, s.count)
	current := s.header.forwards[0]

	for current != nil {
		records = append(records, current.record)
		current = current.forwards[0]
	}

	return records
}
