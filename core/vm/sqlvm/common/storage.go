package common

import (
	"math/big"

	"golang.org/x/crypto/sha3"

	"github.com/dexon-foundation/dexon/common"
	"github.com/dexon-foundation/dexon/core/vm"
	"github.com/dexon-foundation/dexon/core/vm/sqlvm/schema"
	"github.com/dexon-foundation/dexon/crypto"
	"github.com/dexon-foundation/dexon/rlp"
)

// Constants for path keys.
var (
	pathCompTables         = []byte("tables")
	pathCompPrimary        = []byte("primary")
	pathCompIndices        = []byte("indices")
	pathCompSequence       = []byte("sequence")
	pathCompOwner          = []byte("owner")
	pathCompWriters        = []byte("writers")
	pathCompReverseIndices = []byte("reverse_indices")
)

// Storage holds SQLVM required data and method.
type Storage struct {
	vm.StateDB
	Schema schema.Schema
}

// NewStorage return Storage instance.
func NewStorage(state vm.StateDB) *Storage {
	s := &Storage{state, schema.Schema{}}
	return s
}

func (s *Storage) hashPathKey(key [][]byte) (h common.Hash) {
	hw := sha3.NewLegacyKeccak256()
	rlp.Encode(hw, key)
	// length of common.Hash is 256bit,
	// so it can properly match the size of hw.Sum
	hw.Sum(h[:0])
	return
}

// GetRowPathHash return primary key hash which points to row data.
func (s *Storage) GetRowPathHash(tableRef schema.TableRef, rowID uint64) common.Hash {
	// PathKey(["tables", "{table_name}", "primary", uint64({row_id})])
	key := [][]byte{
		pathCompTables,
		tableRefToBytes(tableRef),
		pathCompPrimary,
		uint64ToBytes(rowID),
	}
	return s.hashPathKey(key)
}

// GetIndexValuesPathHash return the hash address to IndexValues structure
// which contains all possible values.
func (s *Storage) GetIndexValuesPathHash(
	tableRef schema.TableRef,
	indexRef schema.IndexRef,
) common.Hash {
	// PathKey(["tables", "{table_name}", "indices", "{index_name}"])
	key := [][]byte{
		pathCompTables,
		tableRefToBytes(tableRef),
		pathCompIndices,
		indexRefToBytes(indexRef),
	}
	return s.hashPathKey(key)
}

// GetIndexEntryPathHash return the hash address to IndexEntry structure for a
// given value.
func (s *Storage) GetIndexEntryPathHash(
	tableRef schema.TableRef,
	indexRef schema.IndexRef,
	values ...[]byte,
) common.Hash {
	// PathKey(["tables", "{table_name}", "indices", "{index_name}", field_1, field_2, field_3, ...])
	key := make([][]byte, 0, 4+len(values))
	key = append(key, pathCompTables, tableRefToBytes(tableRef))
	key = append(key, pathCompIndices, indexRefToBytes(indexRef))
	key = append(key, values...)
	return s.hashPathKey(key)
}

// GetReverseIndexPathHash return the hash address to IndexRev structure for a
// row in a table.
func (s *Storage) GetReverseIndexPathHash(
	tableRef schema.TableRef,
	rowID uint64,
) common.Hash {
	// PathKey(["tables", "{table_name}", "reverse_indices", "{RowID}"])
	key := [][]byte{
		pathCompTables,
		tableRefToBytes(tableRef),
		pathCompReverseIndices,
		uint64ToBytes(rowID),
	}
	return s.hashPathKey(key)
}

// GetPrimaryPathHash returns primary rlp encoded hash.
func (s *Storage) GetPrimaryPathHash(tableRef schema.TableRef) (h common.Hash) {
	// PathKey(["tables", "{table_name}", "primary"])
	key := [][]byte{
		pathCompTables,
		tableRefToBytes(tableRef),
		pathCompPrimary,
	}
	return s.hashPathKey(key)
}

// getSequencePathHash return the hash address of a sequence.
func (s *Storage) getSequencePathHash(
	tableRef schema.TableRef, seqIdx uint8,
) common.Hash {
	// PathKey(["tables", "{table_name}", "sequence", uint8(sequence_idx)])
	key := [][]byte{
		pathCompTables,
		tableRefToBytes(tableRef),
		pathCompSequence,
		uint8ToBytes(seqIdx),
	}
	return s.hashPathKey(key)
}

func (s *Storage) getOwnerPathHash() common.Hash {
	// PathKey(["owner"])
	key := [][]byte{pathCompOwner}
	return s.hashPathKey(key)
}

func (s *Storage) getTableWritersPathHash(tableRef schema.TableRef) common.Hash {
	// PathKey(["tables", "{table_name}", "writers"])
	key := [][]byte{
		pathCompTables,
		tableRefToBytes(tableRef),
		pathCompWriters,
	}
	return s.hashPathKey(key)
}

func (s *Storage) getTableWriterRevIdxPathHash(
	tableRef schema.TableRef,
	account common.Address,
) common.Hash {
	// PathKey(["tables", "{table_name}", "writers", "{addr}"])
	key := [][]byte{
		pathCompTables,
		tableRefToBytes(tableRef),
		pathCompWriters,
		account.Bytes(),
	}
	return s.hashPathKey(key)
}

// ShiftHashUint64 shift hash in uint64.
func (s *Storage) ShiftHashUint64(hash common.Hash, shift uint64) common.Hash {
	bigIntOffset := new(big.Int)
	bigIntOffset.SetUint64(shift)
	return s.ShiftHashBigInt(hash, bigIntOffset)
}

// ShiftHashBigInt shift hash in big.Int
func (s *Storage) ShiftHashBigInt(hash common.Hash, shift *big.Int) common.Hash {
	head := hash.Big()
	head.Add(head, shift)
	return common.BytesToHash(head.Bytes())
}

// ShiftHashListEntry shift hash from the head of a list to the hash of
// idx-th entry.
func (s *Storage) ShiftHashListEntry(
	base common.Hash,
	headerSize uint64,
	entrySize uint64,
	idx uint64,
) common.Hash {
	// TODO(yenlin): tuning when headerSize+entrySize*idx do not overflow.
	shift := new(big.Int)
	operand := new(big.Int)
	shift.SetUint64(entrySize)
	operand.SetUint64(idx)
	shift.Mul(shift, operand)
	operand.SetUint64(headerSize)
	shift.Add(shift, operand)
	return s.ShiftHashBigInt(base, shift)
}

func getDByteSize(data common.Hash) uint64 {
	bytes := data.Bytes()
	lastByte := bytes[len(bytes)-1]
	if lastByte&0x1 == 0 {
		return uint64(lastByte / 2)
	}
	return new(big.Int).Div(new(big.Int).Sub(
		data.Big(), big.NewInt(1)), big.NewInt(2)).Uint64()
}

// DecodeDByteBySlot given contract address and slot return the dynamic bytes data.
func (s *Storage) DecodeDByteBySlot(address common.Address, slot common.Hash) []byte {
	data := s.GetState(address, slot)
	length := getDByteSize(data)
	if length < common.HashLength {
		return data[:length]
	}
	ptr := crypto.Keccak256Hash(slot.Bytes())
	slotNum := (length-1)/common.HashLength + 1
	rVal := make([]byte, slotNum*common.HashLength)
	for i := uint64(0); i < slotNum; i++ {
		start := i * common.HashLength
		copy(rVal[start:start+common.HashLength], s.GetState(address, ptr).Bytes())
		ptr = s.ShiftHashUint64(ptr, 1)
	}
	return rVal[:length]
}

// IncSequence increment value of sequence by inc and return the old value.
func (s *Storage) IncSequence(
	contract common.Address,
	tableRef schema.TableRef,
	seqIdx uint8,
	inc uint64,
) uint64 {
	seqPath := s.getSequencePathHash(tableRef, seqIdx)
	slot := s.GetState(contract, seqPath)
	val := bytesToUint64(slot.Bytes())
	// TODO(yenlin): Check overflow?
	s.SetState(contract, seqPath, common.BytesToHash(uint64ToBytes(val+inc)))
	return val
}
