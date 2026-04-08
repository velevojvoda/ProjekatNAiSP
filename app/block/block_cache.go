package block

import (
	"container/list"
	"errors"
)

var ErrInvalidCacheCapacity = errors.New("cache capacity must be greater than 0")

type cacheNode struct {
	id   BlockID
	data []byte
}

type BlockCache struct {
	capacity int
	items    map[BlockID]*list.Element
	order    *list.List
}

func NewBlockCache(capacity int) (*BlockCache, error) {
	if capacity <= 0 {
		return nil, ErrInvalidCacheCapacity
	}

	return &BlockCache{
		capacity: capacity,
		items:    make(map[BlockID]*list.Element),
		order:    list.New(),
	}, nil
}

func (c *BlockCache) Get(id BlockID) ([]byte, bool) {
	elem, exists := c.items[id]
	if !exists {
		return nil, false
	}

	c.order.MoveToFront(elem)
	node := elem.Value.(*cacheNode)

	return cloneBytes(node.data), true
}

func (c *BlockCache) Put(id BlockID, data []byte) {
	if elem, exists := c.items[id]; exists {
		node := elem.Value.(*cacheNode)
		node.data = cloneBytes(data)
		c.order.MoveToFront(elem)
		return
	}

	if c.order.Len() == c.capacity {
		c.evictLeastRecentlyUsed()
	}

	node := &cacheNode{
		id:   id,
		data: cloneBytes(data),
	}

	elem := c.order.PushFront(node)
	c.items[id] = elem
}

func (c *BlockCache) UpdateIfPresent(id BlockID, data []byte) bool {
	elem, exists := c.items[id]
	if !exists {
		return false
	}

	node := elem.Value.(*cacheNode)
	node.data = cloneBytes(data)
	c.order.MoveToFront(elem)

	return true
}

func (c *BlockCache) Remove(id BlockID) bool {
	elem, exists := c.items[id]
	if !exists {
		return false
	}

	delete(c.items, id)
	c.order.Remove(elem)

	return true
}

func (c *BlockCache) evictLeastRecentlyUsed() {
	elem := c.order.Back()
	if elem == nil {
		return
	}

	node := elem.Value.(*cacheNode)
	delete(c.items, node.id)
	c.order.Remove(elem)
}

func cloneBytes(data []byte) []byte {
	if data == nil {
		return nil
	}

	cloned := make([]byte, len(data))
	copy(cloned, data)
	return cloned
}
