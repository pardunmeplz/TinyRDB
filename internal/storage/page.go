package storage

import (
	"encoding/binary"
	"io"
	"os"
)

// basic page allocator

type PageAllocator struct {
	PageSize int64
	Database *os.File
}

// the first page will hold the metadata for the database
// fiest 8 bytes of the metadata will be an id to a free page (unused page)

func (pageAllocator *PageAllocator) Initialize() error {
	var err error
	// 4 kb page sizes
	pageAllocator.PageSize = 4096
	pageAllocator.Database, err = os.OpenFile("data.db", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	info, err := pageAllocator.Database.Stat()

	if err != nil || info.Size() != 0 {
		return err
	}
	page := make([]byte, pageAllocator.PageSize)
	_, err = pageAllocator.Database.Write(page)

	return err
}

func (pageAllocator *PageAllocator) AllocatePage() (uint64, error) {
	freePage, err := pageAllocator.ReadFreeList()
	if err != nil {
		return 0, err
	}
	if freePage == 0 {
		page := make([]byte, pageAllocator.PageSize)
		info, err := pageAllocator.Database.Stat()
		id := uint64(info.Size() / pageAllocator.PageSize)
		_, err = pageAllocator.Database.Write(page)
		return id, err
	}

	nextPage := make([]byte, 8)
	_, err = pageAllocator.Database.ReadAt(nextPage, int64(freePage)*int64(pageAllocator.PageSize))
	_, err = pageAllocator.Database.WriteAt(nextPage, 0)

	return freePage, err
}

func (pageAllocator *PageAllocator) ReadPage(id uint) ([]byte, error) {
	page := make([]byte, pageAllocator.PageSize)
	_, err := pageAllocator.Database.ReadAt(page, int64(id)*pageAllocator.PageSize)
	return page, err
}

func (pageAllocator *PageAllocator) WritePage(id uint, data []byte) error {
	_, err := pageAllocator.Database.WriteAt(data, int64(id)*pageAllocator.PageSize)
	return err
}

func (pageAllocator *PageAllocator) FreePage(id uint) {
	pageAllocator.WriteFreeList(id)
}

func (pageAllocator *PageAllocator) ReadFreeList() (uint64, error) {
	// 8 bytes of memory
	idBytes := make([]byte, 8)
	_, err := pageAllocator.Database.ReadAt(idBytes, 0)

	if err != nil {
		if err == io.EOF {
			return 0, nil
		}
		return 0, err
	}

	return binary.LittleEndian.Uint64(idBytes), nil
}

func (pageAllocator *PageAllocator) WriteFreeList(id uint) error {
	idOld, err := pageAllocator.ReadFreeList()
	if err != nil {
		return err
	}
	idBytes := make([]byte, 8)

	binary.LittleEndian.PutUint64(idBytes, uint64(idOld))
	_, err = pageAllocator.Database.WriteAt(idBytes, int64(id)*int64(pageAllocator.PageSize))
	if err != nil {
		return err
	}

	binary.LittleEndian.PutUint64(idBytes, uint64(id))
	_, err = pageAllocator.Database.WriteAt(idBytes, 0)
	return err
}
