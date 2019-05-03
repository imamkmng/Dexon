package runtime

import (
	"github.com/dexon-foundation/dexon/core/vm"
	"github.com/dexon-foundation/dexon/core/vm/sqlvm/common"
	se "github.com/dexon-foundation/dexon/core/vm/sqlvm/errors"
)

// Run is runtime entrypoint.
func Run(stateDB vm.StateDB, ins []Instruction, registers []*Operand) (ret []byte, err error) {
	for _, in := range ins {
		// load register
		for i := 0; i < len(in.Input); i++ {
			if !in.Input[i].IsImmediate {
				in.Input[i] = registers[in.Input[i].RegisterIndex]
			}
		}
		in.Registers = registers

		// jump table and run
		jUnit := jumpTable[in.Op]
		in.GasFunc = jUnit.GasFunc
		errCode := jUnit.Func(&common.Context{}, in)
		if errCode != nil {
			err = se.Error{
				Position: in.Position,
				Code:     errCode.(se.ErrorCode),
				Category: se.ErrorCategoryRuntime,
			}
			return nil, err
		}
	}
	// TODO: ret = ABIEncode(ins[len(ins)-1].Output)
	return
}
