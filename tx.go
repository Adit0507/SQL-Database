package main

type KVTX struct {
	db   *KV
	meta []byte	//for rollback
}

// begin a transaction
func (kv *KV) Begin(tx *KVTX) {
	tx.db = kv
	tx.meta = saveMeta(tx.db)
}

// rollback on error
func (kv *KV) Commit(tx *KVTX) error {
	return updateOrRevert(tx.db, tx.meta)
}

// end transaction
func (kv *KV) Abort(tx *KVTX) {
	loadMeta(tx.db, tx.meta)

	// discard temporaries
	tx.db.page.nappend = 0
	tx.db.page.updates = map[uint64][]byte{}
}