package runtime

type jumpUnit struct {
	Func    OpFunction
	GasFunc GasFunction
}

var jumpTable = [256]jumpUnit{
	// 0x10
	ADD: {
		Func:    opAdd,
		GasFunc: constGasFunc(GasArithAdd),
	},
	MUL: {
		Func:    opMul,
		GasFunc: constGasFunc(GasArithMul),
	},
	SUB: {
		Func:    opSub,
		GasFunc: constGasFunc(GasArithAdd),
	},
	DIV: {
		Func:    opDiv,
		GasFunc: constGasFunc(GasArithMul),
	},
	MOD: {
		Func:    opMod,
		GasFunc: constGasFunc(GasArithMul),
	},
	CONCAT: {
		Func:    opConcat,
		GasFunc: constGasFunc(GasMemAlloc),
	},
	NEG: {
		Func:    opNeg,
		GasFunc: constGasFunc(GasArithAdd),
	},

	// 0x20
	LT: {
		Func:    opLt,
		GasFunc: constGasFunc(GasArithCmp),
	},
	GT: {
		Func:    opGt,
		GasFunc: constGasFunc(GasArithCmp),
	},
	EQ: {
		Func:    opEq,
		GasFunc: constGasFunc(GasArithCmp),
	},
	AND: {
		Func:    opAnd,
		GasFunc: constGasFunc(GasBoolCmp),
	},
	OR: {
		Func:    opOr,
		GasFunc: constGasFunc(GasBoolCmp),
	},
	NOT: {
		Func:    opNot,
		GasFunc: constGasFunc(GasBoolCmp),
	},
	UNION: {
		Func:    opUnion,
		GasFunc: constGasFunc(GasMemAlloc + GasMemSwap),
	},
	INTXN: {
		Func:    opIntxn,
		GasFunc: constGasFunc(GasMemAlloc + GasMemSwap),
	},
	LIKE: {
		Func:    opLike,
		GasFunc: constGasFunc(GasMemSearch),
	},

	// 0x30
	REPEATPK: {
		Func:    opRepeatPK,
		GasFunc: constGasFunc(GasStorageRead),
	},

	// 0x40
	ZIP: {
		Func:    opZip,
		GasFunc: constGasFunc(GasMemAlloc),
	},
	FIELD: {
		Func:    opField,
		GasFunc: constGasFunc(GasMemAlloc),
	},
	PRUNE: {
		Func:    opPrune,
		GasFunc: constGasFunc(GasMemFree),
	},
	SORT: {
		Func:    opSort,
		GasFunc: constGasFunc(GasMemSwap),
	},
	FILTER: {
		Func:    opFilter,
		GasFunc: constGasFunc(GasMemAlloc),
	},
	CAST: {
		Func:    opCast,
		GasFunc: dummyGasFunc,
	},
	CUT: {
		Func:    opCut,
		GasFunc: constGasFunc(GasMemFree),
	},
	RANGE: {
		Func:    opRange,
		GasFunc: constGasFunc(GasMemFree),
	},

	// 0x50
	FUNC: {
		Func:    opFunc,
		GasFunc: dummyGasFunc,
	},

	// 0x60
	LOAD: {
		Func:    opLoad,
		GasFunc: constGasFunc(GasStorageRead),
	},
}
