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
const (
	FreeListHeadOffset = 0
	TotalPageOffset    = 8
	PageSizeOffset     = 16
)

func (pageAllocator *PageAllocator) Initialize(file string, pageSize uint64) error {
	var err error
	// 4 kb page sizes
	pageAllocator.PageSize = 4096
	pageAllocator.Database, err = os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}

	info, err := pageAllocator.Database.Stat()
	if err != nil || info.Size() != 0 {
		return err
	}

	page := make([]byte, pageAllocator.PageSize)
	_, err = pageAllocator.Database.Write(page)

	err = pageAllocator.WriteMetadata(FreeListHeadOffset, 0)
	if err != nil {
		return err
	}
	err = pageAllocator.WriteMetadata(TotalPageOffset, 1)
	if err != nil {
		return err
	}
	err = pageAllocator.WriteMetadata(PageSizeOffset, uint64(pageAllocator.PageSize))
	if err != nil {
		return err
	}

	return err
}

func (pageAllocator *PageAllocator) AllocatePage() (uint64, error) {
	freePage, err := pageAllocator.ReadFreeList()
	if err != nil {
		return 0, err
	}
	if freePage == 0 {
		// add page
		page := make([]byte, pageAllocator.PageSize)
		pages, err := pageAllocator.ReadMetadata(TotalPageOffset)
		if err != nil {
			return 0, err
		}

		id := pages
		_, err = pageAllocator.Database.Write(page)

		if err != nil {
			return 0, err
		}

		err = pageAllocator.WriteMetadata(TotalPageOffset, pages+1)
		return id, err
	}
	// reuse page

	nextPage := make([]byte, 8)
	_, err = pageAllocator.Database.ReadAt(nextPage, int64(freePage)*int64(pageAllocator.PageSize))
	if err != nil {
		return 0, err
	}

	err = pageAllocator.WriteFreeList(binary.LittleEndian.Uint64(nextPage))
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

func (pageAllocator *PageAllocator) FreePage(id uint64) error {
	oldId, err := pageAllocator.ReadFreeList()
	if err != nil {
		return err
	}
	err = pageAllocator.WriteFreeList(id)
	if err != nil {
		return err
	}
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, oldId)
	_, err = pageAllocator.Database.WriteAt(data, int64(id)*pageAllocator.PageSize)
	return err
}

func (pageAllocator *PageAllocator) ReadFreeList() (uint64, error) {
	return pageAllocator.ReadMetadata(FreeListHeadOffset)
}

func (pageAllocator *PageAllocator) WriteFreeList(id uint64) error {
	return pageAllocator.WriteMetadata(FreeListHeadOffset, id)
}

func (pageAllocator *PageAllocator) ReadMetadata(offset int64) (uint64, error) {
	data := make([]byte, 8)
	_, err := pageAllocator.Database.ReadAt(data, offset)

	if err != nil {
		if err == io.EOF {
			return 0, nil
		}
		return 0, err
	}

	return binary.LittleEndian.Uint64(data), nil
}

func (pageAllocator *PageAllocator) WriteMetadata(offset int64, data uint64) error {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, data)

	_, err := pageAllocator.Database.WriteAt(bytes, offset)
	return err
}
