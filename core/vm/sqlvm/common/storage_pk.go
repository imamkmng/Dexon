package common

import (
	"encoding/binary"

	"github.com/dexon-foundation/dexon/common"
	"github.com/dexon-foundation/dexon/core/vm/sqlvm/schema"
)

// PK bitmap operations.

func setBit(n byte, pos uint) byte {
	n |= (1 << pos)
	return n
}

func hasBit(n byte, pos uint) bool {
	val := n & (1 << pos)
	return (val > 0)
}

func getOffset(d common.Hash) (offset []uint64) {
	for j, b := range d {
		for i := 0; i < 8; i++ {
			if hasBit(b, uint(i)) {
				offset = append(offset, uint64(j*8+i))
			}
		}
	}
	return
}

// RepeatPK returns primary IDs by table reference.
func (s *Storage) RepeatPK(address common.Address, tableRef schema.TableRef) []uint64 {
	hash := s.GetPrimaryPathHash(tableRef)
	bm := newBitMap(hash, address, s)
	return bm.loadPK()
}

// IncreasePK increases the primary ID and return it.
func (s *Storage) IncreasePK(
	address common.Address,
	tableRef schema.TableRef,
) uint64 {
	hash := s.GetPrimaryPathHash(tableRef)
	bm := newBitMap(hash, address, s)
	return bm.increasePK()
}

// SetPK sets IDs to primary bit map.
func (s *Storage) SetPK(address common.Address, headerHash common.Hash, IDs []uint64) {
	bm := newBitMap(headerHash, address, s)
	bm.setPK(IDs)
}

type bitMap struct {
	storage    *Storage
	headerSlot common.Hash
	headerData common.Hash
	address    common.Address
	dirtySlot  map[uint64]common.Hash
}

func (bm *bitMap) decodeHeader() (lastRowID, rowCount uint64) {
	lastRowID = binary.BigEndian.Uint64(bm.headerData[:8])
	rowCount = binary.BigEndian.Uint64(bm.headerData[8:16])
	return
}

func (bm *bitMap) encodeHeader(lastRowID, rowCount uint64) {
	binary.BigEndian.PutUint64(bm.headerData[:8], lastRowID)
	binary.BigEndian.PutUint64(bm.headerData[8:16], rowCount)
}

func (bm *bitMap) increasePK() uint64 {
	lastRowID, rowCount := bm.decodeHeader()
	lastRowID++
	rowCount++
	bm.encodeHeader(lastRowID, rowCount)
	shift := lastRowID/256 + 1
	slot := bm.storage.ShiftHashUint64(bm.headerSlot, shift)
	data := bm.storage.GetState(bm.address, slot)
	byteShift := (lastRowID & 255) / 8
	data[byteShift] = setBit(data[byteShift], uint(lastRowID&7))
	bm.dirtySlot[shift] = data
	bm.flushAll()
	return lastRowID
}

func (bm *bitMap) flushHeader() {
	bm.storage.SetState(bm.address, bm.headerSlot, bm.headerData)
}

func (bm *bitMap) flushAll() {
	for k, v := range bm.dirtySlot {
		slot := bm.storage.ShiftHashUint64(bm.headerSlot, k)
		bm.storage.SetState(bm.address, slot, v)
	}
	bm.flushHeader()
	bm.dirtySlot = make(map[uint64]common.Hash)
}

func (bm *bitMap) setPK(IDs []uint64) {
	lastRowID, rowCount := bm.decodeHeader()
	for _, id := range IDs {
		if lastRowID < id {
			lastRowID = id
		}
		slotNum := id/256 + 1
		byteLoc := (id & 255) / 8
		bitLoc := uint(id & 7)
		data, exist := bm.dirtySlot[slotNum]
		if !exist {
			slotHash := bm.storage.ShiftHashUint64(bm.headerSlot, slotNum)
			data = bm.storage.GetState(bm.address, slotHash)
		}
		if !hasBit(data[byteLoc], bitLoc) {
			rowCount++
			data[byteLoc] = setBit(data[byteLoc], bitLoc)
		}
		bm.dirtySlot[slotNum] = data
	}
	bm.encodeHeader(lastRowID, rowCount)
	bm.flushAll()
}

func (bm *bitMap) loadPK() []uint64 {
	lastRowID, rowCount := bm.decodeHeader()
	maxSlotNum := lastRowID/256 + 1
	result := make([]uint64, rowCount)
	ptr := 0
	for slotNum := uint64(0); slotNum < maxSlotNum; slotNum++ {
		slotHash := bm.storage.ShiftHashUint64(bm.headerSlot, slotNum+1)
		slotData := bm.storage.GetState(bm.address, slotHash)
		offsets := getOffset(slotData)
		for i, o := range offsets {
			result[i+ptr] = o + slotNum*256
		}
		ptr += len(offsets)
	}
	return result
}

func newBitMap(headerSlot common.Hash, address common.Address, s *Storage) *bitMap {
	headerData := s.GetState(address, headerSlot)
	bm := bitMap{s, headerSlot, headerData, address, make(map[uint64]common.Hash)}
	return &bm
}
