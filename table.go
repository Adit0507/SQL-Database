package main

const (
	TYPE_ERROR = 0
	TYPE_BYTES = 1
	TYPE_INT64 = 2
)

type DB struct {
	Path   string
	kv     KV
	tables map[string]*TableDef
}

type TableDef struct {
	Name   string
	Types  []uint32 //col type
	Cols   []string //col name
	PKeys  int
	Prefix uint32
}

// table cell
type Value struct {
	Type uint32
	I64  int64
	Str  []byte
}

// represents a list of col names and values
type Record struct {
	Cols []string
	Vals []Value
}

func (rec *Record) AddStr(col string, val []byte) *Record {
	rec.Cols = append(rec.Cols, col)
	rec.Vals = append(rec.Vals, Value{Type: TYPE_BYTES, Str: val})

	return rec
}

func (rec *Record) AddInt64(col string, val int64) *Record {
	rec.Cols = append(rec.Cols, col)
	rec.Vals = append(rec.Vals, Value{Type: TYPE_INT64, I64: val})

	return rec
}
func (rec *Record) Get(key string) *Value {
	for i, c := range rec.Cols {
		if c == key {
			return &rec.Vals[i]
		}
	}

	return nil
}

// INTERNAL TABLES
// store metadata
var TDEF_META = &TableDef{
	Prefix: 1,
	Name:   "@meta",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"key", "val"},
	PKeys:  1,
}

// store table schemas
var TDEF_TABLE = &TableDef{
	Prefix: 2,
	Name:   "@table",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"name", "def"},
	PKeys:  1,
}

var INTERNAL_TABLES map[string]*TableDef = map[string]*TableDef{
	"@meta":  TDEF_META,
	"@table": TDEF_TABLE,
}

// reorder records to defined col. order
func reorderRecord(tdef *TableDef, rec Record) ([]Value, error) {
	assert(len(rec.Cols) == len(rec.Vals))
	out := make([]Value, len(tdef.Cols))
	for i, c :=  range tdef.Cols {
		v := rec.Get(c)
		if v == nil {
			continue
		}
		if v.Type != tdef.Types[i] {
			return nil, fmt.Errorf("bad column type: %s", c)
		}
		out[i] =*v
	}

	return out, nil
}

func valuesComplete(tdef *TableDef, vals []Value, n int) error {
	for i, v:= range vals {
		if i < n &&v.Type== 0 {
			return fmt.Errorf("missing column: %s", tdef.Cols[i])
		} else if i >= n && v.Type != 0{
			return fmt.Errorf("extra column: %s", tdef.Cols[i])
		}
	}
	
	return nil
}

// check for missing columns
func checkRecord(tdef *TableDef, rec Record, n int) ([]Value, error) {
	vals, err := reorderRecord(tdef, rec)
	if err != nil {
		return nil, err
	}

	err = valuesComplete(tdef, vals, n)
	if err!= nil {
		return nil, err
	}
	return vals, nil
}

// get a single row by primary key
func dbGet(db *DB, tdef *TableDef, rec *Record) (bool, error) {
	vals, err := checkRecord(tdef, *rec, tdef.PKeys)

	return false, nil
}

// get table schema by naem
func getTableDef(db *DB, name string) *TableDef{
	rec := (&Record{}).AddStr("name", []byte(name))	
	ok, err := db

}

func (db *DB) Get(table string, rec *Record) (bool, error) {
	tdef := 
}


func (db *DB) Insert(table string, rec Record) (bool, error)
func (db *DB) Update(table string, rec Record) (bool, error)
func (db *DB) Delete(table string, rec Record) (bool, error)
func (db *DB) Upsert(table string, rec Record) (bool, error)
