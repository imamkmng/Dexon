package ast

import (
	"fmt"
	"math/big"

	"github.com/shopspring/decimal"

	"github.com/dexon-foundation/dexon/common"
	dec "github.com/dexon-foundation/dexon/core/vm/sqlvm/common/decimal"
	se "github.com/dexon-foundation/dexon/core/vm/sqlvm/errors"
)

var (
	bigIntOne = big.NewInt(1)
	bigIntTen = big.NewInt(10)
)

type decPair struct {
	Min, Max decimal.Decimal
}

var (
	decPairMap = make(map[DataType]decPair)
)

// DataTypeMajor defines type for high byte of DataType.
type DataTypeMajor uint8

// DataTypeMinor defines type for low byte of DataType.
type DataTypeMinor uint8

// DataType defines type for data type encoded.
type DataType uint16

// DataTypeMajor enums.
const (
	DataTypeMajorUnknown DataTypeMajor = iota
	DataTypeMajorSpecial
	DataTypeMajorBool
	DataTypeMajorAddress
	DataTypeMajorInt
	DataTypeMajorUint
	DataTypeMajorFixedBytes
	DataTypeMajorDynamicBytes
	DataTypeMajorFixed  DataTypeMajor = 0x10
	DataTypeMajorUfixed DataTypeMajor = 0x30
)

// DataTypeMinor enums.
const (
	DataTypeMinorDontCare       DataTypeMinor = 0x00
	DataTypeMinorSpecialNull    DataTypeMinor = 0x00
	DataTypeMinorSpecialAny     DataTypeMinor = 0x01
	DataTypeMinorSpecialDefault DataTypeMinor = 0x02
)

// DataTypeUnknown for unknown data type.
const DataTypeUnknown DataType = 0

// DecomposeDataType to major and minor part with given data type.
func DecomposeDataType(t DataType) (DataTypeMajor, DataTypeMinor) {
	return DataTypeMajor(t >> 8), DataTypeMinor(t & 0xff)
}

// ComposeDataType to concrete type with major and minor part.
func ComposeDataType(major DataTypeMajor, minor DataTypeMinor) DataType {
	return (DataType(major) << 8) | DataType(minor)
}

// IsFixedRange checks if major is in range of DataTypeMajorFixed.
func (d DataTypeMajor) IsFixedRange() bool {
	return d >= DataTypeMajorFixed && d-DataTypeMajorFixed <= 0x1f
}

// IsUfixedRange checks if major is in range of DataTypeMajorUfixed.
func (d DataTypeMajor) IsUfixedRange() bool {
	return d >= DataTypeMajorUfixed && d-DataTypeMajorUfixed <= 0x1f
}

// Size return the bytes of the data type occupied.
func (dt DataType) Size() uint8 {
	major, minor := DecomposeDataType(dt)
	if major.IsFixedRange() {
		return uint8(major - DataTypeMajorFixed + 1)
	}
	if major.IsUfixedRange() {
		return uint8(major - DataTypeMajorUfixed + 1)
	}
	switch major {
	case DataTypeMajorBool:
		return 1
	case DataTypeMajorDynamicBytes:
		return common.HashLength
	case DataTypeMajorAddress:
		return common.AddressLength
	case DataTypeMajorInt, DataTypeMajorUint, DataTypeMajorFixedBytes:
		return uint8(minor + 1)
	default:
		panic(fmt.Sprintf("unknown data type %v", dt))
	}
}

// GetNode constructs an AST node from a data type.
func (dt DataType) GetNode() TypeNode {
	major, minor := DecomposeDataType(dt)
	switch major {
	case DataTypeMajorBool:
		return &BoolTypeNode{}
	case DataTypeMajorAddress:
		return &AddressTypeNode{}
	case DataTypeMajorInt:
		if minor <= 0x1f {
			size := (uint32(minor) + 1) * 8
			return &IntTypeNode{Unsigned: false, Size: size}
		}
	case DataTypeMajorUint:
		if minor <= 0x1f {
			size := (uint32(minor) + 1) * 8
			return &IntTypeNode{Unsigned: true, Size: size}
		}
	case DataTypeMajorFixedBytes:
		if minor <= 0x1f {
			size := uint32(minor) + 1
			return &FixedBytesTypeNode{Size: size}
		}
	case DataTypeMajorDynamicBytes:
		return &DynamicBytesTypeNode{}
	}
	switch {
	case major.IsFixedRange():
		if minor <= 80 {
			size := (uint32(major-DataTypeMajorFixed) + 1) * 8
			fractionalDigits := uint32(minor)
			return &FixedTypeNode{
				Unsigned:         false,
				Size:             size,
				FractionalDigits: fractionalDigits,
			}
		}
	case major.IsUfixedRange():
		if minor <= 80 {
			size := (uint32(major-DataTypeMajorUfixed) + 1) * 8
			fractionalDigits := uint32(minor)
			return &FixedTypeNode{
				Unsigned:         true,
				Size:             size,
				FractionalDigits: fractionalDigits,
			}
		}
	}
	return nil
}

func decimalToBig(d decimal.Decimal) (b *big.Int) {
	if exponent := int64(d.Exponent()); exponent >= 0 {
		exp := new(big.Int).Exp(bigIntTen, big.NewInt(exponent), nil)
		b = new(big.Int).Mul(d.Coefficient(), exp)
	} else {
		exp := new(big.Int).Exp(bigIntTen, big.NewInt(-exponent), nil)
		b = new(big.Int).Div(d.Coefficient(), exp)
	}
	return
}

// Don't handle overflow here.
func decimalEncode(size int, d decimal.Decimal) []byte {
	ret := make([]byte, size)
	s := d.Sign()
	if s == 0 {
		return ret
	}

	b := decimalToBig(d)

	if s > 0 {
		bs := b.Bytes()
		copy(ret[size-len(bs):], bs)
		return ret
	}

	b.Add(b, bigIntOne)
	bs := b.Bytes()
	copy(ret[size-len(bs):], bs)
	for idx := range ret {
		ret[idx] = ^ret[idx]
	}
	return ret
}

// Don't handle overflow here.
func decimalDecode(signed bool, bs []byte) decimal.Decimal {
	neg := false
	if signed && (bs[0]&0x80 != 0) {
		neg = true
		for idx := range bs {
			bs[idx] = ^bs[idx]
		}
	}

	b := new(big.Int).SetBytes(bs)

	if neg {
		b.Add(b, bigIntOne)
		b.Neg(b)
	}

	return decimal.NewFromBigInt(b, 0)
}

// DecimalEncode encodes decimal to bytes depend on data type.
func DecimalEncode(dt DataType, d decimal.Decimal) ([]byte, error) {
	major, minor := DecomposeDataType(dt)
	switch major {
	case DataTypeMajorInt,
		DataTypeMajorUint,
		DataTypeMajorFixedBytes:
		return decimalEncode(int(minor)+1, d), nil
	case DataTypeMajorAddress:
		return decimalEncode(common.AddressLength, d), nil
	}
	switch {
	case major.IsFixedRange():
		return decimalEncode(
			int(major-DataTypeMajorFixed)+1,
			d.Shift(int32(minor))), nil
	case major.IsUfixedRange():
		return decimalEncode(
			int(major-DataTypeMajorUfixed)+1,
			d.Shift(int32(minor))), nil
	}

	return nil, se.ErrorCodeDecimalEncode
}

// DecimalDecode decodes decimal from bytes.
func DecimalDecode(dt DataType, b []byte) (decimal.Decimal, error) {
	major, minor := DecomposeDataType(dt)
	switch major {
	case DataTypeMajorInt:
		return decimalDecode(true, b), nil
	case DataTypeMajorUint,
		DataTypeMajorFixedBytes,
		DataTypeMajorAddress:
		return decimalDecode(false, b), nil
	}
	switch {
	case major.IsFixedRange():
		return decimalDecode(true, b).Shift(-int32(minor)), nil
	case major.IsUfixedRange():
		return decimalDecode(false, b).Shift(-int32(minor)), nil
	}

	return decimal.Zero, se.ErrorCodeDecimalDecode
}

// GetMinMax returns min, max pair according to given data type.
func GetMinMax(dt DataType) (min, max decimal.Decimal, err error) {
	cached, ok := decPairMap[dt]
	if ok {
		min, max = cached.Min, cached.Max
		return
	}

	major, minor := DecomposeDataType(dt)
	switch major {
	case DataTypeMajorBool:
		min, max = dec.False, dec.True
	case DataTypeMajorAddress:
		bigUMax := new(big.Int).Lsh(bigIntOne, common.AddressLength*8)
		max = decimal.NewFromBigInt(bigUMax, 0).Sub(dec.One)
	case DataTypeMajorInt:
		bigMax := new(big.Int).Lsh(bigIntOne, (uint(minor)+1)*8-1)
		decMax := decimal.NewFromBigInt(bigMax, 0)
		min, max = decMax.Neg(), decMax.Sub(dec.One)
	case DataTypeMajorUint,
		DataTypeMajorFixedBytes:
		bigUMax := new(big.Int).Lsh(bigIntOne, (uint(minor)+1)*8)
		max = decimal.NewFromBigInt(bigUMax, 0).Sub(dec.One)
	default:
		err = se.ErrorCodeGetMinMax
		return
	}

	decPairMap[dt] = decPair{Max: max, Min: min}
	return
}

// DecimalToUint64 convert decimal to uint64.
// Negative case will return error, and decimal part will be trancated.
func DecimalToUint64(d decimal.Decimal) (uint64, error) {
	s := d.Sign()
	if s == 0 {
		return 0, nil
	}
	if s < 0 {
		return 0, se.ErrorCodeNegDecimalToUint64
	}
	return decimalToBig(d).Uint64(), nil
}
