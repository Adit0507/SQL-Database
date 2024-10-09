package main

const (
    TYPE_ERROR = 0
	TYPE_BYTES = 1
	TYPE_INT64 = 2
)

type TableDef struct {
    Name string
    Types []uint32  //col type
    Cols []string   //col name
    PKeys int
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
    Vals[]Value
}
