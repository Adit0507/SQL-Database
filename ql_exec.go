package main

import (
	"bytes"
	"cmp"
	"fmt"
)

// evaluating expressions
type QLEvalContext struct {
	env Record
	out Value
	err error
}

func qlErr(ctx *QLEvalContext, format string, args ...interface{}) {
	if ctx.err == nil {
		ctx.out.Type = QL_ERR
		ctx.err = fmt.Errorf(format, args...)
	}
}

func b2i(b bool) int64 {
	if b {
		return 1
	} else {
		return 0
	}
}

func qlEval(ctx *QLEvalContext, node QLNODE) {
	switch node.Type {
	// refer to col.
	case QL_SYM:
		if v := ctx.env.Get(string(node.Str)); v != nil {
			ctx.out = *v
		} else {
			qlErr(ctx, "unknown col.: %s", node.Str)
		}
	//literla value
	case QL_I64, QL_STR:
		ctx.out = node.Value
	case QL_TUP:
		qlErr(ctx, "unexpected tuple")

		// operators
	case QL_NEG:
		qlEval(ctx, node.Kids[0])
		if ctx.out.Type == TYPE_INT64 {
			ctx.out.I64 = -ctx.out.I64
		} else {
			qlErr(ctx, "QL_NEG type error")
		}
	case QL_NOT:
		qlEval(ctx, node.Kids[0])
		if ctx.out.Type == TYPE_INT64 {
			ctx.out.I64 = b2i(ctx.out.I64 == 0)
		} else {
			qlErr(ctx, "QL_NOT type error")
		}

		// binary ops.
	case QL_CMP_GE, QL_CMP_GT, QL_CMP_LT, QL_CMP_LE, QL_CMP_EQ, QL_CMP_NE:
		fallthrough
	case QL_ADD, QL_SUB, QL_MUL, QL_DIV, QL_MOD, QL_AND, QL_OR:
		qlBinop(ctx, node)

	default:
		panic("unreachable")

	}
}

// binary operators
func qlBinop(ctx *QLEvalContext, node QLNODE) {
	isCmp := false
	switch node.Type {
	case QL_CMP_GE, QL_CMP_GT, QL_CMP_LT, QL_CMP_LE, QL_CMP_EQ, QL_CMP_NE:
		isCmp = true
	}

	// tuple comparision
	if isCmp && node.Kids[0].Type == QL_TUP && node.Kids[1].Type == QL_TUP {
		r := qlTupleCmp(ctx, node.Kids[0], node.Kids[1])
		ctx.out.Type = QL_I64
		ctx.out.I64 = b2i(cmp2bool(r, node.Type))
		return
	}
}

func cmp2bool(res int, cmd uint32) bool {
	switch cmd {
	case QL_CMP_GE:
		return res >= 0
	case QL_CMP_GT:
		return res > 0
	case QL_CMP_LT:
		return res < 0
	case QL_CMP_LE:
		return res <= 0
	case QL_CMP_EQ:
		return res == 0
	case QL_CMP_NE:
		return res != 0
	
	default:
		panic("unreachable")
	}
}

func qlValueCmp(ctx *QLEvalContext, a1 Value, a2 Value) int {
	switch {
	case ctx.err != nil:
		return 0

	case a1.Type != a2.Type:
		qlErr(ctx, "comparison of different types")
		return 0

	case a1.Type == TYPE_INT64:
		return cmp.Compare(a1.I64, a2.I64)

	case a1.Type == TYPE_BYTES:
		return bytes.Compare(a1.Str, a2.Str)

	default:
		panic("unreachable")
	}
}

// comparin 2 tuples of equal length
func qlTupleCmp(ctx *QLEvalContext, n1 QLNODE, n2 QLNODE) int {
	if len(n1.Kids) != len(n2.Kids) {
		qlErr(ctx, "tuple comp. of different lengths")
	}

	for i := 0; i < len(n1.Kids) && ctx.err == nil; i++ {
		qlEval(ctx, n1.Kids[i])
		a1 := ctx.out
		qlEval(ctx, n2.Kids[i])
		a2 := ctx.out
		if cmp := qlValueCmp(ctx, a1, a2); cmp != 0 {
			return cmp
		}
	}

	return 0
}
