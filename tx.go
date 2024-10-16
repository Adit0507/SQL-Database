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
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	// check conflicts
	if tx.updateAttempted && detectConflicts(kv, tx) {
		return ErrorConflict
	}

	// save meta page
	meta, root := saveMeta(kv), kv.tree.root
	kv.free.curVer = kv.version + 1 //transfer current updates to current tree
	writes := []KeyRange(nil)
	for iter := tx.pending.Seek(nil, CMP_GT); iter.Valid(); iter.Next() {
		modified := false
		key, val := iter.Deref()
		oldVal, isOld := tx.snapshot.Get(key)
		switch val[0] {
		case FLAG_DELETED:
			modified = isOld
			deleted, err := kv.tree.Delete(&DeleteReq{Key: key})
			assert(err == nil)          // can only fail by length limit
			assert(deleted == modified) // assured by conflict detection

		case FLAG_UPDATED:
			modified = (!isOld || !bytes.Equal(oldVal, val[1:]))
			updated, err := kv.tree.Update(&UpdateReq{Key: key, Val: val[1:]})
			assert(err == nil)
			assert(updated == modified)

		default:
			panic("unreachable")
		}

		if modified && len(kv.ongoing) > 1 {
			writes = append(writes, KeyRange{key, key})
		}
	}

	// commitin update
	if root != kv.tree.root {
		kv.version++
		if err := updateOrRevert(kv, meta); err != nil {
			return err
		}
	}

	if len(writes) > 0 {
		slices.SortFunc(writes, func(r1, r2 KeyRange) int {
			return bytes.Compare(r1.start, r2.start)
		})
		kv.history = append(kv.history, CommittedTX{kv.version, writes})
	}
	return nil
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

var ErrorConflict = errors.New("cannot commit due to conflict")

func detectConflicts(kv *KV, tx *KVTX) bool {
	slices.SortFunc(tx.reads, func(r1, r2 KeyRange) int {
		return bytes.Compare(r1.start, r2.start)
	})

	for i := len(kv.history) - 1; i >= 0; i-- {
		if !versionBefore(tx.version, kv.history[i].version) {
			break
		}
		if sortedRangesOverlap(tx.reads, kv.history[i].writes) {
			return true
		}
	}

	return false
}

func sortedRangesOverlap(s1, s2 []KeyRange) bool {
	for len(s1) > 0 && len(s2) > 0 {
		if bytes.Compare(s1[0].stop, s2[0].start) < 0 {
			s1 = s1[1:]
		} else if bytes.Compare(s2[0].stop, s1[0].start) < 0 {
			s2 = s2[1:]
		} else {
			return true
		}
	}

	return false
}

// routines when exiting a transacion
func txFinalize(kv *KV, tx *KVTX) {
	idx := slices.Index(kv.ongoing, tx.version)
	last := len(kv.ongoing) - 1
	kv.ongoing[idx], kv.ongoing = kv.ongoing[last], kv.ongoing[:last]

	// oldest in use version
	minVer := kv.version
	for _, other := range kv.ongoing {
		if versionBefore(other, minVer) {
			minVer = other
		}
	}

	// release free list
	kv.free.SetMaxVer(minVer)

	for idx = 0; idx < len(kv.history); idx++ {
		if versionBefore(minVer, kv.history[idx].version) {
			break
		}
	}

	kv.history = kv.history[idx:]
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