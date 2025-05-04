package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// PageAllocator manages the allocation and deallocation of pages in the database.
// It maintains a free list of pages and handles page metadata including:
// - Page version
// - Page type
// - Checksum for data integrity
type PageAllocator struct {
	PageSize int64    // Size of each page in bytes
	Database *os.File // File handle for the database file
	// Pre-calculated checksum for empty pages to avoid recalculation
	emptyChecksum uint32
}

// Initialize sets up the page allocator by:
// 1. Opening the database file
// 2. Creating the metadata page if the database is new
// 3. Initializing the free list and page count
func (pageAllocator *PageAllocator) Initialize(file string) error {
	// Initialize fields
	pageAllocator.PageSize = DefaultPageSize
	var err error
	pageAllocator.Database, err = os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	data := MakePageData()
	pageAllocator.emptyChecksum = getChecksum(data)

	// Check if database is new (needs metadata page)
	info, err := pageAllocator.Database.Stat()
	if err != nil || info.Size() != 0 {
		return err
	}

	// Create metadata page with headers
	metaData := make([]byte, pageAllocator.PageSize)
	metaData[PageHeaderVersionOffset] = 0
	metaData[PageHeaderTypeOffset] = PagetypeMetadata
	binary.LittleEndian.PutUint32(data[PageHeaderChecksumOffset:], pageAllocator.emptyChecksum)

	// Write metadata page to disk
	_, err = pageAllocator.Database.Write(metaData)
	if err != nil {
		return err
	}

	// Initialize metadata values
	err = pageAllocator.WriteMetadata(MetadataFreeListHeadOffset, 0) // Empty free list
	if err != nil {
		return err
	}
	err = pageAllocator.WriteMetadata(MetadataTotalPageOffset, 1) // One page (metadata)
	if err != nil {
		return err
	}
	err = pageAllocator.WriteMetadata(MetadataPageSizeOffset, uint64(pageAllocator.PageSize))
	if err != nil {
		return err
	}

	return err
}

// AllocatePage allocates a new page of the specified type.
// It first tries to reuse a page from the free list, and if none are available,
// it creates a new page at the end of the database file.
func (pageAllocator *PageAllocator) AllocatePage(pageType byte) (uint64, error) {
	// Try to get a page from the free list
	freePage, err := pageAllocator.ReadFreeList()
	if err != nil {
		return 0, err
	}
	if freePage == 0 {
		// No free pages, create a new one
		data := make([]byte, pageAllocator.PageSize)
		// Set page headers
		data[PageHeaderVersionOffset] = 0
		data[PageHeaderTypeOffset] = pageType
		binary.LittleEndian.PutUint32(data[PageHeaderChecksumOffset:], pageAllocator.emptyChecksum)

		// Get new page ID
		id, err := pageAllocator.ReadMetadata(MetadataTotalPageOffset)
		if err != nil {
			return 0, err
		}

		// Write new page to disk
		_, err = pageAllocator.Database.Write(data)
		if err != nil {
			return 0, err
		}

		// Update total page count
		err = pageAllocator.WriteMetadata(MetadataTotalPageOffset, id+1)
		return id, err
	}

	// Reuse a page from the free list
	nextPage := make([]byte, 8)
	_, err = pageAllocator.Database.ReadAt(nextPage, int64(freePage)*int64(pageAllocator.PageSize)+PageHeaderSize)
	if err != nil {
		return 0, err
	}

	// Update free list to point to next free page
	err = pageAllocator.WriteFreeList(binary.LittleEndian.Uint64(nextPage))
	// Update page type
	pageAllocator.WritePageHeader(freePage, PageHeaderTypeOffset, pageType)
	return freePage, err
}

// FreePage adds a page to the free list for reuse.
// It updates the free list head and marks the page as free.
func (pageAllocator *PageAllocator) FreePage(id uint64) error {
	// Get current free list head
	oldId, err := pageAllocator.ReadFreeList()
	if err != nil {
		return err
	}
	// Update free list to point to this page
	err = pageAllocator.WriteFreeList(id)
	if err != nil {
		return err
	}
	// Write old free list head to this page
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, oldId)
	_, err = pageAllocator.Database.WriteAt(data, int64(id)*pageAllocator.PageSize+PageHeaderSize)
	if err != nil {
		return err
	}
	// Update page metadata
	pageData, err := pageAllocator.readPageDataWithoutVerify(id)
	if err != nil {
		return err
	}
	err = pageAllocator.WritePageHeader(id, PageHeaderChecksumOffset, getChecksum(pageData))
	if err != nil {
		return err
	}
	err = pageAllocator.WritePageHeader(id, PageHeaderTypeOffset, PagetypeFreepage)
	return err
}

// ReadFreeList retrieves the head of the free list from metadata
func (pageAllocator *PageAllocator) ReadFreeList() (uint64, error) {
	return pageAllocator.ReadMetadata(MetadataFreeListHeadOffset)
}

// WriteFreeList updates the head of the free list in metadata
func (pageAllocator *PageAllocator) WriteFreeList(id uint64) error {
	return pageAllocator.WriteMetadata(MetadataFreeListHeadOffset, id)
}

// ReadMetadata reads a 64-bit value from the metadata page at the specified offset
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

// WriteMetadata writes a 64-bit value to the metadata page at the specified offset
// and updates the metadata page checksum
func (pageAllocator *PageAllocator) WriteMetadata(offset int64, data uint64) error {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, data)

	_, err := pageAllocator.Database.WriteAt(bytes, offset)
	if err != nil {
		return err
	}

	// Update metadata page checksum
	pageData, err := pageAllocator.readPageDataWithoutVerify(0)
	if err != nil {
		return err
	}
	err = pageAllocator.WritePageHeader(0, PageHeaderChecksumOffset, getChecksum(pageData))
	return err
}

// ReadPageHeader reads the header information for a page
func (pageAllocator *PageAllocator) ReadPageHeader(id uint64) (PageHeader, error) {
	data := make([]byte, PageHeaderSize)
	_, err := pageAllocator.Database.ReadAt(data, int64(id)*pageAllocator.PageSize)
	response := PageHeader{}
	response.PageVersion = data[PageHeaderVersionOffset]
	response.PageType = data[PageHeaderTypeOffset]
	response.Checksum = binary.LittleEndian.Uint32(data[PageHeaderChecksumOffset:])
	return response, err
}

// WritePageHeader writes a value to a specific offset in a page's header
func (pageAllocator *PageAllocator) WritePageHeader(id uint64, offset int64, header any) error {
	switch header.(type) {
	case byte:
		data, _ := header.(byte)
		_, err := pageAllocator.Database.WriteAt([]byte{data}, int64(id)*pageAllocator.PageSize+offset)
		return err
	case uint32:
		data, _ := header.(uint32)
		dataBytes := make([]byte, 0, 4)
		dataBytes = binary.LittleEndian.AppendUint32(dataBytes, data)
		_, err := pageAllocator.Database.WriteAt(dataBytes, int64(id)*pageAllocator.PageSize+offset)
		return err
	default:
		return nil
	}
}

// WritePageData writes data to a page, starting after the page header
func (pageAllocator *PageAllocator) WritePageData(id uint64, data PageData) error {
	_, err := pageAllocator.Database.WriteAt(data[:], int64(id)*pageAllocator.PageSize+PageHeaderSize)
	if err != nil {
		return err
	}
	// Update page checksum
	return pageAllocator.WritePageHeader(id, PageHeaderChecksumOffset, getChecksum(data))
}

// readPageDataWithoutVerify reads page data without validating its checksum.
// This is used internally when we need to read data to calculate a new checksum.
func (pageAllocator *PageAllocator) readPageDataWithoutVerify(id uint64) (PageData, error) {
	data := MakePageData()
	_, err := pageAllocator.Database.ReadAt(data[:], int64(id)*pageAllocator.PageSize+PageHeaderSize)
	return data, err
}

// ReadPageData reads page data and verifies its integrity using the checksum.
// Returns an error if the checksum doesn't match, indicating data corruption.
func (pageAllocator *PageAllocator) ReadPageData(id uint64) (PageData, error) {
	data := MakePageData()
	_, err := pageAllocator.Database.ReadAt(data[:], int64(id)*pageAllocator.PageSize+PageHeaderSize)
	if err != nil {
		return data, err
	}
	header, err := pageAllocator.ReadPageHeader(id)
	checksum := getChecksum(data)
	if header.Checksum != checksum {
		return data, fmt.Errorf("Checksum Mismatch %d against %d", header.Checksum, checksum)
	}
	return data, err
}

// VerifyDatabase performs a full database integrity check by:
// 1. Reading all pages
// 2. Verifying each page's checksum
// Returns true if all pages are valid, false if any corruption is found.
func (pageAllocator *PageAllocator) VerifyDatabase() (bool, error) {
	count, err := pageAllocator.ReadMetadata(MetadataTotalPageOffset)
	if err != nil {
		return false, err
	}
	for x := range count {
		header, err := pageAllocator.ReadPageHeader(x)
		if err != nil {
			return false, err
		}
		data, err := pageAllocator.readPageDataWithoutVerify(x)
		if err != nil {
			return false, err
		}
		if getChecksum(data) != header.Checksum {
			return false, nil
		}
	}
	return true, nil
}

// CloseFile closes the database file handle
func (PageAllocator *PageAllocator) CloseFile() error {
	err := PageAllocator.Database.Close()
	return err
}
