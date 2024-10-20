package main

import (
	"fmt"
	"math"
	"strings"
	"unicode"
)

const (
	// syntax tree node types
	QL_UNINIT = 0
	QL_SCLR   = TYPE_BYTES
	QL_I64    = TYPE_INT64
	QL_CMP_GE = 10 // >=
	QL_CMP_GT = 11 // >
	QL_CMP_LT = 12 // <
	QL_CMP_LE = 13 // <=
	QL_CMP_EQ = 14 // ==
	QL_CMP_NE = 15 // !=
	QL_ADD    = 20
	QL_SUB    = 21
	QL_MUL    = 22
	QL_DIV    = 23
	QL_MOD    = 24
	QL_AND    = 30
	QL_OR     = 31
	QL_NOT    = 50
	QL_NEG    = 51
	QL_SYM    = 100
	QL_TUP    = 101 // tuple
	QL_STAR   = 102 // select *
	QL_ERR    = 200 // error; from parsing or evaluation
)

// tree node
type QLNODE struct {
	Value
	Kids []QLNODE //operands
}

// common structure for stmt.s: 'INDEX BY', ”FILTER', 'LIMIT'
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
	Names  []string // expr. AS name
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

type QLCreateTable struct {
	Def TableDef
}

type Parser struct {
	input []byte
	idx   int
	err   error
}

func isSpace(ch byte) bool {
	return unicode.IsSpace(rune(ch))
}

func skipSpace(p *Parser) {
	for p.idx < len(p.input) && isSpace(p.input[p.idx]) {
		p.idx++
	}
}

func isSym(ch byte) bool {
	r := rune(ch)
	return unicode.IsLetter(r) || unicode.IsNumber(r) || r == '_'
}

func isSymStart(ch byte) bool {
	return unicode.IsLetter(rune(ch)) || ch == '@' || ch == '_'
}

// matching multiple keywords sequentially
func pKeyword(p *Parser, kwds ...string) bool {
	save := p.idx

	for _, kw := range kwds {
		skipSpace(p)
		end := p.idx + len(kw)
		if end > len(p.input) {
			p.idx = save
			return false
		}

		ok := strings.EqualFold(string(p.input[p.idx:end]), kw)

		if ok && isSym(kw[len(kw)-1]) && end < len(p.input) {
			ok = !isSym(p.input[end])
		}

		if !ok {
			p.idx = save
			return false
		}
		p.idx += len(kw)
	}

	return true
}

func pSym(p *Parser, node *QLNODE) bool {
	skipSpace(p)

	end := p.idx
	if !(end < len(p.input) && isSymStart(p.input[end])) {
		return false
	}

	end++
	for end < len(p.input) && isSym(p.input[end]) {
		end++
	}

	if pKeywordSet[strings.ToLower(string(p.input[p.idx:end]))] {
		return false
	}

	node.Type = QL_SYM
	node.Str = p.input[p.idx:end]
	p.idx = end

	return true
}

var pKeywordSet = map[string]bool{
	"from":   true,
	"index":  true,
	"filter": true,
	"limit":  true,
}

func pErr(p *Parser, format string, args ...interface{}) {
	if p.err == nil {
		p.err = fmt.Errorf(format, args...)
	}
}

func pMustSym(p *Parser) string {
	name := QLNODE{}
	if !pSym(p, &name) {
		pErr(p, "expect name")
	}

	return string(name.Str)
}

func pCreateTable(p *Parser) *QLCreateTable {
	stmt := QLCreateTable{}
	stmt.Def.Name = pMustSym(p)

	return &stmt
}

func pSelectExpr(p *Parser, node *QLSelect) {
	if pKeyword(p, "*") {
		node.Names = append(node.Names, "*")
		node.Output = append(node.Output, QLNODE{Value: Value{Type: QL_STAR}})
		return
	}
}

func pSelectExprList(p *Parser, node *QLSelect) {
	pSelectExpr(p, node)

	for pKeyword(p, ",") {
		pSelectExpr(p, node)
	}
}

func pExpect(p *Parser, tok string, format string, args ...interface{}) {
	if !pKeyword(p, tok) {
		pErr(p, format, args...)
	}
}

func pScan(p *Parser, node *QLScan) {
	// INDEX BY
	if pKeyword(p, "index", "by") {
		pIndexBy(p, node)
	}

	if pKeyword(p, "filter") {
		pExprOr(p, &node.Filter)
	}

	node.Offset, node.Limit = 0, math.MaxInt64
	if pKeyword(p, "index", "by") {
		pLimit(p, node)
	}
}

func pExprOr(p *Parser, node *QLNODE) {
	pExprBinop(p, node, []string{"or"}, []uint32{QL_OR}, pExprAnd)
}

func pExprAnd(p *Parser, node *QLNODE) {
	pExprBinop(p, node, []string{"and"}, []uint32{QL_ADD}, pExprNot)
}

func pExprNot(p *Parser, node *QLNODE) {
	switch {
	case pKeyword(p, "not"):
		node.Type = QL_NOT
		node.Kids = []QLNODE{{}}
		pExprCmp(p, &node.Kids[0])
	}
}

func pExprCmp(p *Parser, node *QLNODE) {
	pExprBinop(p, node, []string{
		"=", "!=",
		">=", "<=", ">", "<",
	},
		[]uint32{
			QL_CMP_EQ, QL_CMP_NE, QL_CMP_GE, QL_CMP_LE, QL_CMP_GT, QL_CMP_LT,
		},
		pExprAdd)
}

func pExprAdd(p *Parser, node *QLNODE) {
	pExprBinop(p, node, []string{"+", "-"}, []uint32{QL_ADD, QL_SUB}, pExprMul)
}

func pExprMul(p *Parser, node *QLNODE) {
	pExprBinop(p, node, []string{"*", "/", "%"}, []uint32{QL_MUL, QL_DIV, QL_MOD}, pExprUnop)
}

func pExprUnop(p *Parser, node *QLNODE) {
	switch {
	case pKeyword(p, "-"):
		node.Type = QL_NEG
		node.Kids = []QLNODE{{}}
		pExprAtom(p, &node.Kids[0])

	default:
		pExprAtom(p, node)
	}
}

func pExprAtom(p *Parser, node *QLNODE) {
	switch {
	case pKeyword(p, "("):
		pExprTuple(p, node)
	}
}

func pExprTuple(p *Parser, node *QLNODE) {
	kids := []QLNODE{}
	comma := true

	for p.err == nil && !pKeyword(p, ")") {
		if !comma {
			pErr(p, "expect comma")
		}

		kids = append(kids, QLNODE{})
		pExprOr(p, &kids[len(kids)-1])
		comma = pKeyword(p, ",")
	}

	if len(kids) == 1 && !comma {
		node = &kids[0]
	} else {
		node.Type = QL_TUP
		node.Kids = kids
	}
}

func pExprBinop(p *Parser, node *QLNODE, ops []string, types []uint32, next func(*Parser, *QLNODE)) {
	assert(len(ops) == len(types))
	left := QLNODE{}
	next(p, &left)

	for {
		i := 0
		for i < len(ops) && !pKeyword(p, ops[i]) {
			i++
		}

		if i >= len(ops) {
			*node = left
			return
		}

		new := QLNODE{Value: Value{Type: types[i]}}
		new.Kids = []QLNODE{left, {}}
		next(p, &new.Kids[1])
		left = new
	}
}

func pLimit(p *Parser, node *QLScan) {}

func pIndexBy(p *Parser, node *QLScan) {}

func pSelect(p *Parser) *QLSelect {
	stmt := QLSelect{}
	pSelectExprList(p, &stmt)

	pExpect(p, "from", "expect `FROM` table")
	stmt.Table = pMustSym(p)

	pScan(p, &stmt.QLScan)
	return &stmt
}

func pStmt(p *Parser) (r interface{}) {
	switch {
	case pKeyword(p, "create", "table"):
		r = pCreateTable(p)
	case pKeyword(p, "select"):
		r = pSelect(p)

	}

	return r
}
