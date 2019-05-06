package common

import (
	"math/big"

	"github.com/dexon-foundation/decimal"
	"github.com/dexon-foundation/dexon/common"
	"github.com/dexon-foundation/dexon/core/vm/sqlvm/ast"
	"github.com/dexon-foundation/dexon/core/vm/sqlvm/schema"
)

// TODO(yenlin): Do we really need to use ast encode/decode here?
func uint64ToBytes(id uint64) []byte {
	bigIntID := new(big.Int).SetUint64(id)
	decimalID := decimal.NewFromBigInt(bigIntID, 0)
	dt := ast.ComposeDataType(ast.DataTypeMajorUint, 7)
	byteID, _ := ast.DecimalEncode(dt, decimalID)
	return byteID
}

func bytesToUint64(b []byte) uint64 {
	dt := ast.ComposeDataType(ast.DataTypeMajorUint, 7)
	d, _ := ast.DecimalDecode(dt, b)
	// TODO(yenlin): Not yet a convenient way to extract uint64 from decimal...
	bigInt := d.Rescale(0).Coefficient()
	return bigInt.Uint64()
}

func uint8ToBytes(i uint8) []byte {
	return []byte{i}
}

func tableRefToBytes(t schema.TableRef) []byte {
	return uint8ToBytes(uint8(t))
}

func columnRefToBytes(c schema.ColumnRef) []byte {
	return uint8ToBytes(uint8(c))
}

func indexRefToBytes(i schema.IndexRef) []byte {
	return uint8ToBytes(uint8(i))
}

func hashToAddress(hash common.Hash) common.Address {
	return common.BytesToAddress(hash.Bytes())
}

func addressToHash(addr common.Address) common.Hash {
	return common.BytesToHash(addr.Bytes())
}
