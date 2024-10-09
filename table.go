package main

const (
	TYPE_ERROR = 0
	TYPE_BYTES = 1
	TYPE_INT64 = 2
)

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
	for i, c := range rec.Cols{
		if c  == key{
			return &rec.Vals[i]
		}
	}

	return nil
}