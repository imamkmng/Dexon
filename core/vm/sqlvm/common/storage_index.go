package common

import (
	"github.com/dexon-foundation/dexon/common"
	"github.com/dexon-foundation/dexon/core/vm/sqlvm/schema"
)

// Index related operations of Storage.

// IndexValues contain addresses to all possible values of an index.
type IndexValues struct {
	// Header.
	Length uint64
	// 3 unused uint64 fields here.
	// Contents.
	ValueHashes []common.Hash
}

// IndexEntry contain row ids of a given value in an index.
type IndexEntry struct {
	// Header.
	Length              uint64
	IndexToValuesOffset uint64
	ForeignKeyRefCount  uint64
	// 1 unused uint64 field here.
	// Contents.
	RowIDs []uint64
}

// LoadIndexValues load IndexValues struct of a given index.
func (s *Storage) LoadIndexValues(
	contract common.Address,
	tableRef schema.TableRef,
	indexRef schema.IndexRef,
	onlyHeader bool,
) *IndexValues {
	ret := &IndexValues{}
	slot := s.GetIndexValuesPathHash(tableRef, indexRef)
	data := s.GetState(contract, slot)
	ret.Length = bytesToUint64(data[:8])
	if onlyHeader {
		return ret
	}
	// Load all ValueHashes.
	ret.ValueHashes = make([]common.Hash, ret.Length)
	for i := uint64(0); i < ret.Length; i++ {
		slot = s.ShiftHashUint64(slot, 1)
		ret.ValueHashes[i] = s.GetState(contract, slot)
	}
	return ret
}

// LoadIndexEntry load IndexEntry struct of a given value key on an index.
func (s *Storage) LoadIndexEntry(
	contract common.Address,
	tableRef schema.TableRef,
	indexRef schema.IndexRef,
	onlyHeader bool,
	values ...[]byte,
) *IndexEntry {
	ret := &IndexEntry{}
	slot := s.GetIndexEntryPathHash(tableRef, indexRef, values...)
	data := s.GetState(contract, slot)
	ret.Length = bytesToUint64(data[:8])
	ret.IndexToValuesOffset = bytesToUint64(data[8:16])
	ret.ForeignKeyRefCount = bytesToUint64(data[16:24])

	if onlyHeader {
		return ret
	}
	// Load all RowIDs.
	ret.RowIDs = make([]uint64, 0, ret.Length)
	remain := ret.Length
	for remain > 0 {
		bound := remain
		if bound > 4 {
			bound = 4
		}
		slot = s.ShiftHashUint64(slot, 1)
		data := s.GetState(contract, slot).Bytes()
		for i := uint64(0); i < bound; i++ {
			ret.RowIDs = append(ret.RowIDs, bytesToUint64(data[:8]))
			data = data[8:]
		}
		remain -= bound
	}
	return ret
}
