package runtime

import (
	"github.com/dexon-foundation/dexon/core/vm/sqlvm/common"
	se "github.com/dexon-foundation/dexon/core/vm/sqlvm/errors"
)

// GasFunction accepts interface and returns consumed gas.
type GasFunction func(v interface{}) (consumed uint64)

var (
	dummyGasFunc = func(v interface{}) uint64 { return 0 }
)

func applyGas(ctx *common.Context, fn GasFunction, v interface{}) (err error) {
	consumed := fn(v)
	if consumed > ctx.GasLimit {
		err = se.ErrorCodeOutOfGas
		return
	}
	ctx.GasLimit -= consumed
	return
}

// Shared gas variables
const (
	GasArithAdd     = 2
	GasArithMul     = 3
	GasArithCmp     = 3
	GasBoolCmp      = 3
	GasMemAlloc     = 10
	GasMemFree      = 5 // GC work
	GasMemSwap      = 5
	GasMemSearch    = 20
	GasStorageRead  = 30
	GasStorageWrite = 50
	GasBitCmp       = 5
)

func constGasFunc(c uint64) GasFunction {
	return func(v interface{}) uint64 { return c * v.(uint64) }
}
