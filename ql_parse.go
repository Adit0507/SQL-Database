package main

import (
	"fmt"
	"math"
	"strings"
	"unicode"
)

const (
	QL_SYM = 100
	QL_STAR = 102
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

func pExpect(p*Parser, tok string, format string, args ...interface{}) {
	if !pKeyword(p, tok) {
		pErr(p, format, args...)
	}
}

func pScan(p *Parser, node *QLScan){
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

func pExprOr(p *Parser, node *QLNODE) {}

func pLimit(p *Parser, node *QLScan){}

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
