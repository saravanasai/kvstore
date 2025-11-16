package main

import (
	"encoding/binary"
	"errors"
)

// ============================================================================
// CONSTANTS
// ============================================================================

const (
	PageSize      = 4096
	HeaderSize    = 16
	SlotArrSize   = 6
	KeySize       = 2
	ValueSize     = 2
	MaxKeyBytes   = 400
	MaxValueBytes = 400
)

// ============================================================================
// TYPES
// ============================================================================

type SlotArr struct {
	offset uint16
	len    uint16
	flag   uint16
}

// Page Layout (4096 bytes total)
// ┌──────────────────────────────────────────────────────────────────────────────────────┐
// │                                 HEADER SECTION (16 bytes)                            │
// ├─────────────────┬─────────────────┬─────────────────┬──────────────────────────────┤
// │     PageId      │     Count       │   FreeSpace     │      DataStart               │
// │   (uint64)      │   (uint32)      │   (uint16)      │      (uint16)                │
// │    8 bytes      │    4 bytes      │    2 bytes      │      2 bytes                 │
// ├─────────────────┼─────────────────┼─────────────────┼──────────────────────────────┤
// │  Offset 0-7     │  Offset 8-11    │  Offset 12-13   │      Offset 14-15            │
// └─────────────────┴─────────────────┴─────────────────┴──────────────────────────────┘
//
// ┌──────────────────────────────────────────────────────────────────────────────────────┐
// │                          DATA SECTION (4080 bytes)                                   │
// │                         Ptr [PageSize - HeaderSize]byte                              │
// ├──────────────────────────────────────────────────────────────────────────────────────┤
// │                                                                                      │
// │  ┌─────────────────────────────────────────────────────────┐                    │
// │  │              SLOT ARRAY (grows from left →)                 │                    │
// │  ├────────────┬────────────┬────────────┬─────────────────────┤                    │
// │  │  Slot 0    │  Slot 1    │  Slot 2    │  Slot N ...         │                    │
// │  │  (6 bytes) │  (6 bytes) │  (6 bytes) │  (6 bytes)          │                    │
// │  ├────────────┼────────────┼────────────┼─────────────────────┤                    │
// │  │ offset(2)  │ offset(2)  │ offset(2)  │ offset(2)           │                    │
// │  │ len(2)     │ len(2)     │ len(2)     │ len(2)              │                    │
// │  │ flag(2)    │ flag(2)    │ flag(2)    │ flag(2)             │                    │
// │  └────────────┴────────────┴────────────┴─────────────────────┘                    │
// │                                                                                      │
// │                          FREE SPACE                                                  │
// │                  (between slot array and records)                                    │
// │                                                                                      │
// │  ┌─────────────────────────────────────────────────────────────┐                    │
// │  │           RECORDS (grow from right ←)                       │                    │
// │  │                                                             │                    │
// │  │  ... [Record 2][Record 1][Record 0]                         │                    │
// │  │                                                             │                    │
// │  │  Each Record Format:                                        │                    │
// │  │  ┌──────────┬────────────┬─────────┬──────────┐            │                    │
// │  │  │ KeySize  │ ValueSize  │   Key   │  Value   │            │                    │
// │  │  │ (uint16) │  (uint16)  │ (var)   │  (var)   │            │                    │
// │  │  │ 2 bytes  │  2 bytes   │ N bytes │ M bytes  │            │                    │
// │  │  └──────────┴────────────┴─────────┴──────────┘            │                    │
// │  └─────────────────────────────────────────────────────────────┘                    │
// │                                                                                      │
// └──────────────────────────────────────────────────────────────────────────────────────┘
//
// Memory Layout Example (after 2 inserts):
//
// Header: [PageId=1][Count=2][FreeSpace=4050][DataStart=4062]
//
// Ptr[0:6]    → Slot 0: offset=4062, len=10, flag=0
// Ptr[6:12]   → Slot 1: offset=4052, len=10, flag=0
// Ptr[12:...] → Free Space
// ...
// Ptr[4052:4062] → Record 1: [keySize=1][valueSize=5]["b"]["world"]
// Ptr[4062:4072] → Record 0: [keySize=1][valueSize=5]["a"]["hello"]
//
// Notes:
// - Slot array starts at Ptr[0] and grows rightward
// - Records start at Ptr[PageSize-HeaderSize-1] and grow leftward
// - DataStart points to where next record will be written (in Ptr coordinates)
// - FreeSpace = DataStart - (Count * SlotArrSize)

type Page struct {
	PageId    uint64                      // 8 bytes
	Count     uint32                      // 4 bytes
	FreeSpace uint16                      // 2 bytes
	DataStart uint16                      // 2 bytes
	Ptr       [PageSize - HeaderSize]byte // PageSize - HeaderSize
}

type DatabaseMeta struct {
	NextPageId uint64
	PageCount  uint64
	LastPageId uint64
}

type PageManager struct {
	Pages    []Page // In-memory page cache
	Disk     Disk   // Disk operations
	MetaData DatabaseMeta
}

// ============================================================================
// PAGE METHODS - Slot Array Management
// ============================================================================

func (p *Page) GetSlot(index int) SlotArr {
	slotOffset := index * SlotArrSize
	return SlotArr{
		offset: binary.LittleEndian.Uint16(p.Ptr[slotOffset : slotOffset+2]),
		len:    binary.LittleEndian.Uint16(p.Ptr[slotOffset+2 : slotOffset+4]),
		flag:   binary.LittleEndian.Uint16(p.Ptr[slotOffset+4 : slotOffset+6]),
	}
}

func (p *Page) SetSlot(index int, slot SlotArr) {
	slotOffset := index * SlotArrSize
	binary.LittleEndian.PutUint16(p.Ptr[slotOffset:slotOffset+2], slot.offset)
	binary.LittleEndian.PutUint16(p.Ptr[slotOffset+2:slotOffset+4], slot.len)
	binary.LittleEndian.PutUint16(p.Ptr[slotOffset+4:slotOffset+6], slot.flag)
}

// ============================================================================
// PAGE METHODS - Record Operations
// ============================================================================

func (p *Page) WriteRecord(key string, value string) error {
	keyBytes := []byte(key)
	valueBytes := []byte(value)

	if len(keyBytes) > MaxKeyBytes {
		return errors.New("key size exceeds maximum allowed")
	}
	if len(valueBytes) > MaxValueBytes {
		return errors.New("value size exceeds maximum allowed")
	}

	recordSize := KeySize + ValueSize + len(keyBytes) + len(valueBytes)

	// Check if we have space for both slot and data
	if int(p.FreeSpace) < recordSize+SlotArrSize {
		return errors.New("not enough space")
	}

	// Initialize DataStart if this is the first record
	if p.Count == 0 {
		p.DataStart = PageSize - HeaderSize
	}

	newDataStart := p.DataStart - uint16(recordSize)
	writePos := int(newDataStart)

	// Write record data (from right to left)
	binary.LittleEndian.PutUint16(p.Ptr[writePos:writePos+2], uint16(len(keyBytes)))
	writePos += 2
	binary.LittleEndian.PutUint16(p.Ptr[writePos:writePos+2], uint16(len(valueBytes)))
	writePos += 2
	copy(p.Ptr[writePos:writePos+len(keyBytes)], keyBytes)
	writePos += len(keyBytes)
	copy(p.Ptr[writePos:writePos+len(valueBytes)], valueBytes)

	slot := SlotArr{
		offset: newDataStart,
		len:    uint16(recordSize),
		flag:   0, // Active
	}
	p.SetSlot(int(p.Count), slot)

	// Update page metadata
	p.DataStart = newDataStart
	p.Count++
	p.FreeSpace -= uint16(recordSize + SlotArrSize)

	return nil
}

func (p *Page) ReadRecord(key string) (string, bool) {
	keyBytes := []byte(key)

	for i := uint32(0); i < p.Count; i++ {
		slot := p.GetSlot(int(i))

		// Skip deleted records
		if slot.flag != 0 {
			continue
		}

		pos := int(slot.offset)

		keySize := binary.LittleEndian.Uint16(p.Ptr[pos : pos+2])
		pos += 2
		valueSize := binary.LittleEndian.Uint16(p.Ptr[pos : pos+2])
		pos += 2

		recordKey := p.Ptr[pos : pos+int(keySize)]
		pos += int(keySize)
		recordValue := p.Ptr[pos : pos+int(valueSize)]

		if string(recordKey) == string(keyBytes) {
			return string(recordValue), true
		}
	}

	return "", false
}

func (p *Page) HasSpace(recordSize int) bool {
	return int(p.FreeSpace) >= recordSize
}

// ============================================================================
// PAGE MANAGER METHODS - Initialization
// ============================================================================

func NewPageManager(disk *Disk) *PageManager {
	return &PageManager{
		Pages: make([]Page, 0),
		Disk:  *disk,
		MetaData: DatabaseMeta{
			NextPageId: 1,
			PageCount:  0,
			LastPageId: 1,
		},
	}
}

func (pm *PageManager) CreatePage() *Page {

	page := &Page{
		PageId:    pm.MetaData.NextPageId,
		Count:     0,
		FreeSpace: PageSize - HeaderSize,
		Ptr:       [PageSize - HeaderSize]byte{},
	}

	pm.MetaData.LastPageId = pm.MetaData.NextPageId
	pm.MetaData.NextPageId = pm.MetaData.LastPageId + 1
	pm.MetaData.PageCount++

	return page
}
func (pm *PageManager) LoadMetaPage() error {

	buf, err := pm.Disk.Read(0, PageSize)
	if err != nil {
		return err
	}

	nextPageId := binary.LittleEndian.Uint64(buf[0:8])
	pageCount := binary.LittleEndian.Uint64(buf[8:16])
	lastPageId := binary.LittleEndian.Uint64(buf[16:24])

	pm.MetaData.NextPageId = nextPageId
	pm.MetaData.PageCount = pageCount
	pm.MetaData.LastPageId = lastPageId

	return nil
}

func (pm *PageManager) SaveMetaDataPage() error {

	buf := make([]byte, PageSize)

	binary.LittleEndian.PutUint64(buf[0:8], pm.MetaData.NextPageId)
	binary.LittleEndian.PutUint64(buf[8:16], pm.MetaData.PageCount)
	binary.LittleEndian.PutUint64(buf[16:24], pm.MetaData.LastPageId)

	// Write to page 0 (metadata page)
	_, err := pm.Disk.Write(0, buf)
	return err
}

func (pm *PageManager) LoadPage(pageId uint64) (*Page, error) {

	pageOffset := int((pageId) * PageSize)

	// Read raw page data
	buf, err := pm.Disk.Read(pageOffset, PageSize)
	if err != nil {
		return nil, err
	}

	// Parse page header with little endian
	pageIdFromDisk := binary.LittleEndian.Uint64(buf[0:8])
	count := binary.LittleEndian.Uint32(buf[8:12])
	freeSpace := binary.LittleEndian.Uint16(buf[12:14])
	dataStart := binary.LittleEndian.Uint16(buf[14:16])

	page := &Page{
		PageId:    pageIdFromDisk,
		Count:     count,
		FreeSpace: freeSpace,
		DataStart: dataStart,
	}

	// Copy data section
	copy(page.Ptr[:], buf[HeaderSize:])

	return page, nil

}

func (pm *PageManager) InsertRecord(key string, value string) error {
	recordSize := KeySize + ValueSize + len(key) + len(value)

	pageWithSpace, err := pm.findPageWithSpace(recordSize)
	if err != nil {

		pageWithSpace = pm.CreatePage()
		// Write the new empty page to disk first
		err = pm.writePageToDisk(pageWithSpace)
		if err != nil {
			return err
		}
		// Save metadata after creating new page
		pm.SaveMetaDataPage()
	}

	err = pageWithSpace.WriteRecord(key, value)
	if err != nil {
		return err
	}

	return pm.writePageToDisk(pageWithSpace)
}

func (pm *PageManager) writePageToDisk(page *Page) error {
	// Convert page struct to bytes
	buf := make([]byte, PageSize)

	// Write header
	binary.LittleEndian.PutUint64(buf[0:8], page.PageId)
	binary.LittleEndian.PutUint32(buf[8:12], page.Count)
	binary.LittleEndian.PutUint16(buf[12:14], page.FreeSpace)
	binary.LittleEndian.PutUint16(buf[14:16], page.DataStart)

	copy(buf[HeaderSize:], page.Ptr[:])

	// Write to disk at correct offset
	pageOffset := int((page.PageId) * PageSize)
	_, err := pm.Disk.Write(pageOffset, buf)
	return err
}

func (pm *PageManager) findPageWithSpace(recordSize int) (*Page, error) {
	// Loop through existing pages (1 to LastPageId, skip page 0 which is metadata)
	for pageId := uint64(1); pageId <= pm.MetaData.LastPageId; pageId++ {
		page, err := pm.LoadPage(pageId)
		if err != nil {
			continue // Skip corrupted pages
		}

		if page.HasSpace(recordSize) {
			return page, nil
		}
	}
	return nil, errors.New("no page with enough space")
}

func (pm *PageManager) FindRecord(key string) (string, error) {
	// Search through all existing pages
	for pageId := uint64(1); pageId <= pm.MetaData.LastPageId; pageId++ {
		page, err := pm.LoadPage(pageId)
		if err != nil {
			continue // Skip corrupted pages
		}

		value, found := page.ReadRecord(key)
		if found {
			return value, nil
		}
	}
	return "", errors.New("key not found")
}
