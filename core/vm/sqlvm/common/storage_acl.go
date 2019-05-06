package common

import (
	"github.com/dexon-foundation/dexon/common"
	"github.com/dexon-foundation/dexon/core/vm/sqlvm/schema"
)

// Owner, writer operations of Storage.

// LoadOwner load the owner of a SQLVM contract from storage.
func (s *Storage) LoadOwner(contract common.Address) common.Address {
	return hashToAddress(s.GetState(contract, s.getOwnerPathHash()))
}

// StoreOwner save the owner of a SQLVM contract to storage.
func (s *Storage) StoreOwner(contract, newOwner common.Address) {
	s.SetState(contract, s.getOwnerPathHash(), addressToHash(newOwner))
}

type tableWriters struct {
	Length uint64
	// 3 unused uint64 in slot 1.
	Writers []common.Address // Each address consumes one slot, right aligned.
}

type tableWriterRevIdx struct {
	IndexToValuesOffset uint64
	// 3 unused uint64 in the slot.
}

func (c *tableWriterRevIdx) Valid() bool {
	return c.IndexToValuesOffset != 0
}

func (s *Storage) loadTableWriterRevIdx(
	contract common.Address,
	path common.Hash,
) *tableWriterRevIdx {
	ret := &tableWriterRevIdx{}
	data := s.GetState(contract, path)
	ret.IndexToValuesOffset = bytesToUint64(data[:8])
	return ret
}

func (s *Storage) storeTableWriterRevIdx(
	contract common.Address,
	path common.Hash,
	rev *tableWriterRevIdx,
) {
	var data common.Hash // One slot.
	copy(data[:8], uint64ToBytes(rev.IndexToValuesOffset))
	s.SetState(contract, path, data)
}

func (s *Storage) loadTableWriters(
	contract common.Address,
	pathHash common.Hash,
	onlyHeader bool,
) *tableWriters {
	ret := &tableWriters{}
	header := s.GetState(contract, pathHash)
	ret.Length = bytesToUint64(header[:8])
	if onlyHeader {
		return ret
	}
	ret.Writers = make([]common.Address, ret.Length)
	for i := uint64(0); i < ret.Length; i++ {
		ret.Writers[i] = s.loadSingleTableWriter(contract, pathHash, i)
	}
	return ret
}

func (s *Storage) storeTableWritersHeader(
	contract common.Address,
	pathHash common.Hash,
	w *tableWriters,
) {
	var header common.Hash
	copy(header[:8], uint64ToBytes(w.Length))
	s.SetState(contract, pathHash, header)
}

func (s *Storage) shiftTableWriterList(
	base common.Hash,
	idx uint64,
) common.Hash {
	return s.ShiftHashListEntry(base, 1, 1, idx)
}

func (s *Storage) loadSingleTableWriter(
	contract common.Address,
	writersPathHash common.Hash,
	idx uint64,
) common.Address {
	slot := s.shiftTableWriterList(writersPathHash, idx)
	acc := s.GetState(contract, slot)
	return hashToAddress(acc)
}

func (s *Storage) storeSingleTableWriter(
	contract common.Address,
	writersPathHash common.Hash,
	idx uint64,
	acc common.Address,
) {
	slot := s.shiftTableWriterList(writersPathHash, idx)
	s.SetState(contract, slot, addressToHash(acc))
}

// IsTableWriter check if an account is writer to the table.
func (s *Storage) IsTableWriter(
	contract common.Address,
	tableRef schema.TableRef,
	account common.Address,
) bool {
	path := s.getTableWriterRevIdxPathHash(tableRef, account)
	rev := s.loadTableWriterRevIdx(contract, path)
	return rev.Valid()
}

// LoadTableWriters load writers of a table.
func (s *Storage) LoadTableWriters(
	contract common.Address,
	tableRef schema.TableRef,
) (ret []common.Address) {
	path := s.getTableWritersPathHash(tableRef)
	writers := s.loadTableWriters(contract, path, false)
	return writers.Writers
}

// InsertTableWriter insert an account into writer list of the table.
func (s *Storage) InsertTableWriter(
	contract common.Address,
	tableRef schema.TableRef,
	account common.Address,
) {
	revPath := s.getTableWriterRevIdxPathHash(tableRef, account)
	rev := s.loadTableWriterRevIdx(contract, revPath)
	if rev.Valid() {
		return
	}
	path := s.getTableWritersPathHash(tableRef)
	writers := s.loadTableWriters(contract, path, true)
	// Store modification.
	s.storeSingleTableWriter(contract, path, writers.Length, account)
	writers.Length++
	s.storeTableWritersHeader(contract, path, writers)
	// Notice: IndexToValuesOffset starts from 1.
	s.storeTableWriterRevIdx(contract, revPath, &tableWriterRevIdx{
		IndexToValuesOffset: writers.Length,
	})
}

// DeleteTableWriter delete an account from writer list of the table.
func (s *Storage) DeleteTableWriter(
	contract common.Address,
	tableRef schema.TableRef,
	account common.Address,
) {
	revPath := s.getTableWriterRevIdxPathHash(tableRef, account)
	rev := s.loadTableWriterRevIdx(contract, revPath)
	if !rev.Valid() {
		return
	}
	path := s.getTableWritersPathHash(tableRef)
	writers := s.loadTableWriters(contract, path, true)

	// Store modification.
	if rev.IndexToValuesOffset != writers.Length {
		// Move last to deleted slot.
		lastAcc := s.loadSingleTableWriter(contract, path, writers.Length-1)
		s.storeSingleTableWriter(contract, path, rev.IndexToValuesOffset-1,
			lastAcc)
		s.storeTableWriterRevIdx(contract, s.getTableWriterRevIdxPathHash(
			tableRef, lastAcc), rev)
	}
	// Delete last.
	writers.Length--
	s.storeTableWritersHeader(contract, path, writers)
	s.storeSingleTableWriter(contract, path, writers.Length, common.Address{})
	s.storeTableWriterRevIdx(contract, revPath, &tableWriterRevIdx{})
}
