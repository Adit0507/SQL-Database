package main

type BIter struct {
	tree *BTree
	path []BNode
	pos  []uint16
}

// movin backward & forward
func (iter *BIter) Next() {
	iterNext(iter, len(iter.path)-1)
}

func iterIsFirst(iter *BIter) bool {
	for _, pos := range iter.pos {
		if pos != 0 {
			return false
		}
	}
	return true
}

func iterPrev(iter *BIter, level int) {
	if iter.pos[level] > 0 {
		iter.pos[level]-- //move within node
	} else if level > 0 {
		iterPrev(iter, level-1) //move to sibling noe
	} else {
		panic("unreachable")
	}

	if level+1 < len(iter.pos) {
		node := iter.path[level]
		kid := BNode(iter.tree.get(node.getPtr(iter.pos[level])))
		iter.path[level+1] = kid
		iter.pos[level+1] = kid.nkeys() - 1
	}
}

func (iter *BIter) Prev() {
	if !iterIsFirst(iter) {
		iterPrev(iter, len(iter.path)-1)
	}
}

func iterNext(iter *BIter, level int) {
	if iter.pos[level]+1 < iter.path[level].nkeys() {
		iter.pos[level]++ //move within node
	} else if level > 0 {
		iterNext(iter, level-1) //move to sibling node
	} else {
		iter.pos[len(iter.pos)-1]++ //past last key
		return
	}
	if level+1 < len(iter.pos) { //update child node
		node := iter.path[level]
		kid := BNode(iter.tree.get(node.getPtr(iter.pos[level])))
		iter.path[level+1] = kid
		iter.pos[level+1] = 0
	}
}

// find closest position that is less or equal to input key
func (tree *BTree) SeekLE(key []byte) *BIter {
	iter := &BIter{tree: tree}
	for ptr := tree.root; ptr != 0; {
		node := BNode(tree.get(ptr))
		idx := nodeLookupLE(node, key)
		iter.path = append(iter.path, node)
		iter.pos = append(iter.pos, idx)
		ptr = node.getPtr(idx)
	}
	return iter
}

// get current KV pair
func (iter *BIter) Deref() ([]byte, []byte)

// precondition of Dref()
func (iter *BIter) Valid() bool
