package storage

import (
	"crypto/rand"
	"os"
	"testing"
)

func newAllocator(t *testing.T) *PageAllocator {
	os.Remove("test.db")

	pageAllocator := &PageAllocator{}
	err := pageAllocator.Initialize("test.db")
	if err != nil {
		t.Fatal("Failed to initialize page allocator:", err)
	}
	return pageAllocator
}

func estReadWrite(t *testing.T) {
	const PageCount = 5
	pageAllocator := newAllocator(t)

	// Allocate a few pages
	pageIDs := []uint64{}
	for i := 0; i < PageCount; i++ {
		pageID, err := pageAllocator.AllocatePage(PagetypeUserdata)
		if err != nil {
			t.Fatal("Page allocation failed:", err)
		}
		pageIDs = append(pageIDs, pageID)
	}

	// Write random data to pages
	pageData := make(map[uint64]PageData)
	for _, id := range pageIDs {
		data := MakePageData()
		rand.Read(data[:])

		err := pageAllocator.WritePageData(id, data)
		if err != nil {
			t.Fatal("Write failed for page", id, ":", err)
		}
		pageData[id] = data
	}

	// Read back and verify the data
	for _, id := range pageIDs {
		readData, err := pageAllocator.ReadPageData(id)
		if err != nil {
			t.Fatal("Read failed for page", id, ":", err)
		}

		if string(readData[:]) != string(pageData[id][:]) {
			t.Error("Data mismatch for page", id)
		}
	}
}

func estReuseOnAllocate(t *testing.T) {
	pageAllocator := newAllocator(t)

	// get a page
	id, err := pageAllocator.AllocatePage(PagetypeUserdata)
	if err != nil {
		t.Fatal("Failed to allocate page:", err)
	}

	// Free a page
	err = pageAllocator.WriteFreeList(id)
	if err != nil {
		t.Fatal("Failed to free page", id, ":", err)
	}

	// Allocate another page, should reuse the freed one
	newPage, err := pageAllocator.AllocatePage(PagetypeUserdata)
	if err != nil {
		t.Fatal("Failed to allocate after freeing:", err)
	}

	if newPage != id {
		t.Fatal("Unexpected allocation order! Expected", id, "but got", newPage)
	}

}

func estMetadata(t *testing.T) {
	pageAllocator := newAllocator(t)

	id, err := pageAllocator.AllocatePage(PagetypeUserdata)
	if err != nil {
		t.Fatal("Failed to allocate page:", err)
	}

	offset, err := pageAllocator.ReadMetadata(MetadataTotalPageOffset)
	if err != nil {
		t.Fatal("Failed to read offset", err)
	}
	// one for metadata page and one for the new allocated page
	if offset != 2 {
		t.Error("Failed offset count, Expected 1 but got ", offset)
	}

	err = pageAllocator.FreePage(id)
	if err != nil {
		t.Fatal("Failed to free page ", id, ":", err)
	}

	newId, err := pageAllocator.ReadMetadata(MetadataFreeListHeadOffset)
	if err != nil {
		t.Fatal("Failed to read offset", err)
	}
	if newId != id {
		t.Error("Failed free head metadata check, Expected ", id, "but got", newId)
	}

	pageSize, err := pageAllocator.ReadMetadata(MetadataPageSizeOffset)
	if err != nil {
		t.Fatal("Failed to read offset", err)
	}
	if pageSize != uint64(pageAllocator.PageSize) {
		t.Error("Failed page size metadata check, Expected ", pageAllocator.PageSize, "but got", pageSize)
	}

}
