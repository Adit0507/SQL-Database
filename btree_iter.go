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

func (iter *BIter) Prev()

func iterNext(iter *BIter, level int) {
	if iter.pos[level]+1 < iter.path[level].nkeys() {
		iter.pos[level]++
	} else if level > 0 {
		iterNext(iter, level-1)
	} else {
		iter.pos[len(iter.pos)-1]++
		return
	}
}

// find closest position that is less or equal to input key
func (tree *BTree) SeekLE(key []byte) *BIter

// get current KV pair
func (iter *BIter) Deref() ([]byte, []byte)

// precondition of Dref()
func (iter *BIter) Valid() bool
