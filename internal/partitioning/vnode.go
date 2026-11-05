package partitioning

import (
	"sort"
	"strconv"
)

// Hasher maps a key or virtual-node identifier to a position on the ring.
//
// Hashers must be deterministic: every node in a cluster must map the same
// input to the same 64-bit value. DefaultHash satisfies this requirement.
type Hasher func(string) uint64

// DefaultHash returns a well-distributed 64-bit hash of key.
//
// It applies the 64-bit FNV-1a function and then a SplitMix64 finalizer. FNV
// alone has weak avalanche behavior for keys that differ only in their last
// few characters, which is common in keys such as "user:1", "user:2", and
// "partition:17". The finalizer removes that clustering while keeping the
// implementation deterministic, dependency-free, and fast.
func DefaultHash(key string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)

	hash := uint64(offset64)
	for i := 0; i < len(key); i++ {
		hash ^= uint64(key[i])
		hash *= prime64
	}

	// SplitMix64 finalizer.
	hash ^= hash >> 30
	hash *= 0xbf58476d1ce4e5b9
	hash ^= hash >> 27
	hash *= 0x94d049bb133111eb
	hash ^= hash >> 31
	return hash
}

// vnode is one point on the consistent hash ring.
//
// Several virtual nodes share one physical node. The index identifies which
// virtual node produced the token, which keeps duplicate tokens stable when a
// custom hasher maps several identifiers to the same position.
type vnode struct {
	token  uint64
	nodeID NodeID
	index  int
}

// vnodeKey builds the ring input for one virtual node.
//
// The NUL separator prevents ("node1", 2) and ("node", 12) from producing the
// same input. Node IDs may contain arbitrary text, including NUL bytes, so the
// index is appended after a separator rather than concatenated without a
// boundary.
func vnodeKey(nodeID NodeID, index int) string {
	return string(nodeID) + "\x00" + strconv.Itoa(index)
}

// partitionKey builds the ring input used to assign a fixed partition to a
// node. The prefix reduces accidental overlap with node virtual-node inputs.
func partitionKey(partition PartitionID) string {
	return "partition\x00" + strconv.FormatUint(uint64(partition), 10)
}

// buildTokens creates and sorts the virtual-node list for nodes.
//
// Sorting by token first, then node ID and virtual-node index, makes routing
// independent of the caller's node order and deterministic even when two
// identifiers hash to the same token.
func buildTokens(nodes []Node, virtualNodes int, hash Hasher) []vnode {
	tokens := make([]vnode, 0, len(nodes)*virtualNodes)
	for _, node := range nodes {
		for index := 0; index < virtualNodes; index++ {
			tokens = append(tokens, vnode{
				token:  hash(vnodeKey(node.ID, index)),
				nodeID: node.ID,
				index:  index,
			})
		}
	}

	sort.Slice(tokens, func(i, j int) bool {
		if tokens[i].token != tokens[j].token {
			return tokens[i].token < tokens[j].token
		}
		if tokens[i].nodeID != tokens[j].nodeID {
			return tokens[i].nodeID < tokens[j].nodeID
		}
		return tokens[i].index < tokens[j].index
	})
	return tokens
}

// lookupToken returns the index of the first token greater than or equal to
// hash. The result is len(tokens) when the hash belongs after the last token,
// which callers wrap to the start of the ring.
func lookupToken(tokens []vnode, hash uint64) int {
	return sort.Search(len(tokens), func(i int) bool {
		return tokens[i].token >= hash
	})
}
