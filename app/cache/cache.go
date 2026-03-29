package cache

type Node struct {
	Key   string
	Value []byte
	Prev  *Node
	Next  *Node
}

type LRUCache struct {
	Capacity int
	Data     map[string]*Node
	Head     *Node
	Tail     *Node
}

func NewLRUCache(capacity int) *LRUCache {
	if capacity <= 0 {
		capacity = 1
	}

	return &LRUCache{
		Capacity: capacity,
		Data:     make(map[string]*Node),
		Head:     nil,
		Tail:     nil,
	}
}

func (c *LRUCache) Get(key string) ([]byte, bool) {
	node, exists := c.Data[key]
	if !exists {
		return nil, false
	}

	c.MoveToFront(node)
	return node.Value, true
}

func (c *LRUCache) Put(key string, value []byte) {
	node, exists := c.Data[key]

	if exists {
		node.Value = value
		c.MoveToFront(node)
		return
	}

	newNode := &Node{
		Key:   key,
		Value: value,
	}

	c.Data[key] = newNode
	c.AddToFront(newNode)

	if len(c.Data) > c.Capacity {
		c.RemoveTail()
	}
}

func (c *LRUCache) Delete(key string) {
	node, exists := c.Data[key]
	if !exists {
		return
	}

	c.RemoveNode(node)
	delete(c.Data, key)
}

func (c *LRUCache) MoveToFront(node *Node) {
	if node == c.Head {
		return
	}

	c.RemoveNode(node)
	c.AddToFront(node)
}

func (c *LRUCache) AddToFront(node *Node) {
	node.Prev = nil
	node.Next = c.Head

	if c.Head != nil {
		c.Head.Prev = node
	}

	c.Head = node

	if c.Tail == nil {
		c.Tail = node
	}
}

func (c *LRUCache) RemoveNode(node *Node) {
	if node.Prev != nil {
		node.Prev.Next = node.Next
	} else {
		c.Head = node.Next
	}

	if node.Next != nil {
		node.Next.Prev = node.Prev
	} else {
		c.Tail = node.Prev
	}

	node.Prev = nil
	node.Next = nil
}

func (c *LRUCache) RemoveTail() {
	if c.Tail == nil {
		return
	}

	oldTail := c.Tail
	c.RemoveNode(oldTail)
	delete(c.Data, oldTail.Key)
}
