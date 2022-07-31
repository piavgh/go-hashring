package consistent

import (
	"errors"
	"hash/crc32"
	"sort"
	"sync"
)

var (
	ErrNodeNotFound = errors.New("node not found")
)

// Ring is a network of distributed nodes
type Ring struct {
	Nodes Nodes
	mu    sync.Mutex
}

// Initialize new distribute network of nodes or a ring
func NewRing() *Ring {
	return &Ring{
		Nodes: Nodes{},
	}
}

// Add node to the ring
func (r *Ring) AddNode(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	node := NewNode(id)
	r.Nodes = append(r.Nodes, *node)

	sort.Sort(r.Nodes)
}

// Remove node from the ring if it exists, else returns ErrNodeNotFound
func (r *Ring) RemoveNode(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	i := r.search(id)

	if i >= r.Nodes.Len() || r.Nodes[i].Id != id {
		return ErrNodeNotFound
	}

	r.Nodes = append(r.Nodes[:i], r.Nodes[i+1:]...)

	return nil
}

func (r *Ring) search(id string) int {
	searchFn := func(i int) bool {
		return r.Nodes[i].HashId >= hashId(id)
	}

	return sort.Search(r.Nodes.Len(), searchFn)
}

// Get node which is mapped to the key. Return value is identifier of the node given in AddNode
func (r *Ring) Get(key string) string {
	i := r.search(key)

	if i >= r.Nodes.Len() {
		i = 0
	}

	return r.Nodes[i].Id
}

//----------------------------------------------------------
// Node
//----------------------------------------------------------

type Node struct {
	Id     string
	HashId uint32
}

func NewNode(id string) *Node {
	return &Node{
		Id:     id,
		HashId: hashId(id),
	}
}

// Nodes is an array of Node
type Nodes []Node

func (n Nodes) Len() int {
	return len(n)
}

func (n Nodes) Less(i, j int) bool {
	return n[i].HashId < n[j].HashId
}

func (n Nodes) Swap(i, j int) {
	n[i], n[j] = n[j], n[i]
}

//----------------------------------------------------------
// Helpers
//----------------------------------------------------------

func hashId(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}
