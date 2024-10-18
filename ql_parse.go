package main

// tree node
type QLNODE struct {
	Value
	Kids []QLNODE //operands
}

// common structure for stmt.s: 'INDEX BY', ''FILTER', 'LIMIT'
type QLScan struct {
	Table  string
	Key1   QLNODE //index by
	Key2   QLNODE
	Filter QLNODE //filter expression
	Offset int64
	Limit  int64
}

// statements: SELECT UPDATE DELETE
type QLSelect struct {
	QLScan
	Names  []string	// expr. AS name
	Output []QLNODE
}

type QLUPdate struct {
	QLScan
	Names  []string
	Values []QLNODE
}

type QLDelete struct {
	QLScan
}
