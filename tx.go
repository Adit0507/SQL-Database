package main

import "runtime"

type KVTX struct {
	db   *KV
	meta []byte //for rollback
	root uint64
	done bool
}

// begin a transaction
func (kv *KV) Begin(tx *KVTX) {
	tx.db = kv
	tx.meta = saveMeta(tx.db)
	tx.root = tx.db.tree.root
	assert(kv.page.nappend == 0 && len(kv.page.updates) == 0)
	runtime.SetFinalizer(tx, func(x *KVTX) {
		assert(tx.done)
	})
}

// rollback on error
func (kv *KV) Commit(tx *KVTX) error {
	assert(!tx.done)
	tx.done = true
	if kv.tree.root == tx.root{
		return nil
	}

	return updateOrRevert(tx.db, tx.meta)
}

// end transaction
func (kv *KV) Abort(tx *KVTX) {
	assert(!tx.done)
	tx.done = true
	loadMeta(tx.db, tx.meta)

	// discard temporaries
	tx.db.page.nappend = 0
	tx.db.page.updates = map[uint64][]byte{}
}

// KV interfaces
func (tx*KVTX) Seek(key []byte, cmp int) *BIter {
	return tx.db.tree.Seek(key, cmp)
}

func (tx*KVTX) Update(req *UpdateReq) (bool, error) {
	return tx.db.tree.Update(req)
}

func (tx*KVTX) Del(req *DeleteReq) (bool, error) {
	return tx.db.tree.Delete(req)
}

func (tx*KVTX) Set(key []byte, val []byte) (bool, error) {
	return tx.Update(&UpdateReq{Key: key, Val: val})
}

func (tx*KVTX) Get(key []byte)	([]byte, bool) {
	return tx.db.tree.Get(key)
}