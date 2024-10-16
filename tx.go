package main

import "runtime"

type KVTX struct {
	snapshot BTree // read-only snapshot(copy on wrrite)
	version  uint64
	// local updates are held in an memory B+tree
	pending BTree      //captured KV updates
	reads   []KeyRange //list of involved interval of keys for dtecting conflicts
	// cheks for conflict even if update changes nothing
	updateAttempted bool
	done            bool
}

// start <=key <=stop
type KeyRange struct {
	start []byte
	stop []byte
}

const (
	FLAG_DELETED = byte(1)
	FLAG_UPDATED = byte(2)
)

// begin a transaction
func (kv *KV) Begin(tx *KVTX) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	tx.snapshot.root = kv.tree.root
	chunks := kv.mmap.chunks
	tx.snapshot.get = func(ptr uint64) []byte { return mmapRead(ptr, chunks) }
	tx.version = kv.version

	// in memeory tree to caputre updaets
	pages := [][]byte(nil)
	tx.pending.get = func(ptr uint64) []byte { return pages[ptr-1] }
	tx.pending.new = func(b []byte) uint64 {
		pages = append(pages, b)
		return uint64(len(pages))
	}
	tx.pending.del = func(uint64) {}
	// keepin track of concurrent TXs
	kv.ongoing = append(kv.ongoing, tx.version)
	runtime.SetFinalizer(tx, func(tx *KVTX) { assert(tx.done) })
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