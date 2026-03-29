package memtable

import "ProjekatNAiSP/app/model"

const defaultBTreeDegree = 2

type bTreeNode struct {
	leaf     bool
	keys     []string
	records  []model.Record
	children []*bTreeNode
}

type BTreeMemtable struct {
	root             *bTreeNode
	degree           int
	maxEntries       int
	maxSizeBytes     int
	sizeType         string
	currentSizeBytes int
	count            int
}

func NewBTreeMemtable(maxEntries int, maxSizeKB int, sizeType string) *BTreeMemtable {
	return &BTreeMemtable{
		root: &bTreeNode{
			leaf: true,
		},
		degree:       defaultBTreeDegree,
		maxEntries:   maxEntries,
		maxSizeBytes: maxSizeKB * 1024,
		sizeType:     sizeType,
	}
}

func (b *BTreeMemtable) Get(key string) (*model.Record, bool) {
	return b.search(b.root, key)
}

func (b *BTreeMemtable) search(node *bTreeNode, key string) (*model.Record, bool) {
	i := 0
	for i < len(node.keys) && key > node.keys[i] {
		i++
	}

	if i < len(node.keys) && key == node.keys[i] {
		record := node.records[i]
		return &record, true
	}

	if node.leaf {
		return nil, false
	}

	return b.search(node.children[i], key)
}

func (b *BTreeMemtable) Put(key string, value []byte) error {
	record := model.Record{
		Key:       key,
		Value:     value,
		Tombstone: false,
	}
	return b.upsert(record)
}

func (b *BTreeMemtable) Delete(key string) error {
	record := model.Record{
		Key:       key,
		Value:     nil,
		Tombstone: true,
	}
	return b.upsert(record)
}

func (b *BTreeMemtable) upsert(record model.Record) error {
	if oldRecord, found := b.Get(record.Key); found {
		b.currentSizeBytes -= recordSize(*oldRecord)
		b.updateRecord(b.root, record)
		b.currentSizeBytes += recordSize(record)
		return nil
	}

	root := b.root
	if len(root.keys) == 2*b.degree-1 {
		newRoot := &bTreeNode{
			leaf:     false,
			children: []*bTreeNode{root},
		}
		b.splitChild(newRoot, 0)
		b.root = newRoot
		b.insertNonFull(newRoot, record)
	} else {
		b.insertNonFull(root, record)
	}

	b.count++
	b.currentSizeBytes += recordSize(record)
	return nil
}

func (b *BTreeMemtable) updateRecord(node *bTreeNode, record model.Record) bool {
	i := 0
	for i < len(node.keys) && record.Key > node.keys[i] {
		i++
	}

	if i < len(node.keys) && record.Key == node.keys[i] {
		node.records[i] = record
		return true
	}

	if node.leaf {
		return false
	}

	return b.updateRecord(node.children[i], record)
}

func (b *BTreeMemtable) insertNonFull(node *bTreeNode, record model.Record) {
	i := len(node.keys) - 1

	if node.leaf {
		node.keys = append(node.keys, "")
		node.records = append(node.records, model.Record{})

		for i >= 0 && record.Key < node.keys[i] {
			node.keys[i+1] = node.keys[i]
			node.records[i+1] = node.records[i]
			i--
		}

		node.keys[i+1] = record.Key
		node.records[i+1] = record
		return
	}

	for i >= 0 && record.Key < node.keys[i] {
		i--
	}
	i++

	if len(node.children[i].keys) == 2*b.degree-1 {
		b.splitChild(node, i)
		if record.Key > node.keys[i] {
			i++
		}
	}

	b.insertNonFull(node.children[i], record)
}

func (b *BTreeMemtable) splitChild(parent *bTreeNode, index int) {
	t := b.degree
	fullChild := parent.children[index]

	newChild := &bTreeNode{
		leaf: fullChild.leaf,
	}

	midKey := fullChild.keys[t-1]
	midRecord := fullChild.records[t-1]

	newChild.keys = append(newChild.keys, fullChild.keys[t:]...)
	newChild.records = append(newChild.records, fullChild.records[t:]...)

	fullChild.keys = fullChild.keys[:t-1]
	fullChild.records = fullChild.records[:t-1]

	if !fullChild.leaf {
		newChild.children = append(newChild.children, fullChild.children[t:]...)
		fullChild.children = fullChild.children[:t]
	}

	parent.children = append(parent.children, nil)
	copy(parent.children[index+2:], parent.children[index+1:])
	parent.children[index+1] = newChild

	parent.keys = append(parent.keys, "")
	parent.records = append(parent.records, model.Record{})
	copy(parent.keys[index+1:], parent.keys[index:])
	copy(parent.records[index+1:], parent.records[index:])

	parent.keys[index] = midKey
	parent.records[index] = midRecord
}

func (b *BTreeMemtable) IsFull() bool {
	if b.sizeType == "kb" {
		return b.currentSizeBytes >= b.maxSizeBytes
	}
	return b.count >= b.maxEntries
}

func (b *BTreeMemtable) Records() []model.Record {
	var records []model.Record
	b.inOrder(b.root, &records)
	return records
}

func (b *BTreeMemtable) inOrder(node *bTreeNode, records *[]model.Record) {
	if node == nil {
		return
	}

	for i := 0; i < len(node.keys); i++ {
		if !node.leaf {
			b.inOrder(node.children[i], records)
		}
		*records = append(*records, node.records[i])
	}

	if !node.leaf {
		b.inOrder(node.children[len(node.children)-1], records)
	}
}
