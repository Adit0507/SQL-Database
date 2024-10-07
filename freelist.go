package main

import "encoding/binary"

// node format:
// |next| pointers | unused
type LNode []byte

const FREE_LIST_HEADER = 8
const FREE_LIST_CAP = (BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8

// settin& gettin
func (node LNode) getNext() uint64 {
    return binary.LittleEndian.Uint64(node[0:8])
}
func (node LNode) setNext(next uint64) {
    binary.LittleEndian.PutUint64(node[0:8], next)
}
func (node LNode) getPtr(idx int) uint64 {
    offset := FREE_LIST_HEADER + 8*idx
    return binary.LittleEndian.Uint64(node[offset:])
}
func (node LNode) setPtr(idx int, ptr uint64) {
    assert(idx < FREE_LIST_CAP)
    offset := FREE_LIST_HEADER + 8*idx
    binary.LittleEndian.PutUint64(node[offset:], ptr)
}

