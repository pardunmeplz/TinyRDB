package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// basic page allocator
type PageAllocator struct {
	PageSize int64
	Database *os.File
	// calculate at start since it will be reused often
	emptyChecksum uint32
}

func (pageAllocator *PageAllocator) Initialize(file string) error {

	// initialize fields
	pageAllocator.PageSize = DefaultPageSize
	var err error
	pageAllocator.Database, err = os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	data := MakePageData()
	pageAllocator.emptyChecksum = getChecksum(data)

	// check if databse has metadata page
	info, err := pageAllocator.Database.Stat()
	if err != nil || info.Size() != 0 {
		return err
	}

	// add new page
	_, err = pageAllocator.AllocatePage(PagetypeMetadata)

	err = pageAllocator.WriteMetadata(MetadataFreeListHeadOffset, 0)
	if err != nil {
		return err
	}
	err = pageAllocator.WriteMetadata(MetadataTotalPageOffset, 1)
	if err != nil {
		return err
	}
	err = pageAllocator.WriteMetadata(MetadataPageSizeOffset, uint64(pageAllocator.PageSize))
	if err != nil {
		return err
	}

	return err
}

func (pageAllocator *PageAllocator) AllocatePage(pageType byte) (uint64, error) {
	freePage, err := pageAllocator.ReadFreeList()
	if err != nil {
		return 0, err
	}
	if freePage == 0 {
		// add whole page with headers
		data := make([]byte, pageAllocator.PageSize)
		// set headers
		data[PageHeaderVersionOffset] = 0
		data[PageHeaderTypeOffset] = pageType
		binary.BigEndian.PutUint32(data[PageHeaderChecksumOffset:], pageAllocator.emptyChecksum)

		// get new id
		id, err := pageAllocator.ReadMetadata(MetadataTotalPageOffset)
		if err != nil {
			return 0, err
		}

		// allocate page
		_, err = pageAllocator.Database.Write(data)
		if err != nil {
			return 0, err
		}

		// update page count in metadata
		err = pageAllocator.WriteMetadata(MetadataTotalPageOffset, id+1)
		return id, err
	}

	// reuse page
	nextPage := make([]byte, 8)
	_, err = pageAllocator.Database.ReadAt(nextPage, int64(freePage)*int64(pageAllocator.PageSize)+PageHeaderSize)
	if err != nil {
		return 0, err
	}

	err = pageAllocator.WriteFreeList(binary.LittleEndian.Uint64(nextPage))
	pageAllocator.WritePageHeader(freePage, PageHeaderTypeOffset, pageType)
	return freePage, err
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
	data := make([]byte, 0, PageHeaderSize+8)
	data = append(data, 0, PagetypeFreepage)
	binary.LittleEndian.AppendUint64(data, oldId)
	_, err = pageAllocator.Database.WriteAt(data, int64(id)*pageAllocator.PageSize)
	return err
}

func (pageAllocator *PageAllocator) ReadFreeList() (uint64, error) {
	return pageAllocator.ReadMetadata(MetadataFreeListHeadOffset)
}

func (pageAllocator *PageAllocator) WriteFreeList(id uint64) error {
	return pageAllocator.WriteMetadata(MetadataFreeListHeadOffset, id)
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

	_, err := pageAllocator.Database.WriteAt(bytes, offset+PageHeaderSize)
	return err
}

func (pageAllocator *PageAllocator) ReadPageHeader(id uint64) (PageHeader, error) {
	data := make([]byte, PageHeaderSize)
	_, err := pageAllocator.Database.ReadAt(data, int64(id)*pageAllocator.PageSize)
	response := PageHeader{}
	response.PageVersion = data[PageHeaderVersionOffset]
	response.PageType = data[PageHeaderTypeOffset]
	response.Checksum = binary.BigEndian.Uint32(data[PageHeaderChecksumOffset:])
	return response, err
}

func (pageAllocator *PageAllocator) WritePageHeader(id uint64, offset int64, header any) error {
	switch header.(type) {
	case byte:
		data, _ := header.(byte)
		_, err := pageAllocator.Database.WriteAt([]byte{data}, int64(id)*pageAllocator.PageSize+offset)
		return err
	case uint32:
		data := make([]byte, 0, 4)
		binary.BigEndian.AppendUint32(data, pageAllocator.emptyChecksum)
		_, err := pageAllocator.Database.WriteAt(data, int64(id)*pageAllocator.PageSize+offset)
		return err
	default:
		return nil
	}
}

func (pageAllocator *PageAllocator) WritePageData(id uint64, data PageData) error {
	_, err := pageAllocator.Database.WriteAt(data[:], int64(id)*pageAllocator.PageSize+PageHeaderSize)
	if err != nil {
		return err
	}
	err = pageAllocator.WritePageHeader(id, PageHeaderChecksumOffset, getChecksum(data))
	return err
}

func (pageAllocator *PageAllocator) ReadPageData(id uint64) (PageData, error) {
	data := MakePageData()
	_, err := pageAllocator.Database.ReadAt(data[:], int64(id)*pageAllocator.PageSize+PageHeaderSize)
	if err != nil {
		return data, err
	}
	header, err := pageAllocator.ReadPageHeader(id)
	if header.Checksum != getChecksum(data) {
		return data, fmt.Errorf("Checksum Mismatch")
	}
	return data, err
}
