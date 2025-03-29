package storage

import (
	"crypto/rand"
	"os"
	"testing"
)

func TestPageAllocator(t *testing.T) {

	// Remove any existing DB file (for a fresh start)
	os.Remove("test.db")

	// Create and initialize the PageAllocator
	pageAllocator := &PageAllocator{}
	err := pageAllocator.Initialize("test.db")
	if err != nil {
		t.Fatal("Failed to initialize page allocator:", err)
	}

	// Allocate a few pages
	pageIDs := []uint64{}
	for i := 0; i < 5; i++ {
		pageID, err := pageAllocator.AllocatePage()
		if err != nil {
			t.Fatal("Page allocation failed:", err)
		}
		pageIDs = append(pageIDs, pageID)
		t.Log("Allocated Page ID:", pageID)
	}

	// Write random data to pages
	pageData := make(map[uint64][]byte)
	for _, id := range pageIDs {
		data := make([]byte, pageAllocator.PageSize)
		rand.Read(data) // Fill with random data
		err := pageAllocator.WritePage(id, data)
		if err != nil {
			t.Fatal("Write failed for page", id, ":", err)
		}
		pageData[id] = data
	}

	// Read back and verify the data
	for _, id := range pageIDs {
		readData, err := pageAllocator.ReadPage(id)
		if err != nil {
			t.Fatal("Read failed for page", id, ":", err)
		}

		if string(readData) != string(pageData[id]) {
			t.Fatal("Data mismatch for page", id)
		} else {
			t.Log("Page", id, "data verified!")
		}
	}

	t.Log("All allocated pages verified successfully.")

	// Free a page
	toFree := pageIDs[2]
	t.Log("Freeing Page:", toFree)
	err = pageAllocator.WriteFreeList(toFree)
	if err != nil {
		t.Fatal("Failed to free page", toFree, ":", err)
	}

	// Allocate another page, should reuse the freed one
	newPage, err := pageAllocator.AllocatePage()
	if err != nil {
		t.Fatal("Failed to allocate after freeing:", err)
	}
	t.Log("Newly allocated page ID:", newPage)

	// If our free list logic is correct, newPage should be equal to toFree
	if newPage == toFree {
		t.Log("Page reuse confirmed!")
	} else {
		t.Fatal("Unexpected allocation order! Expected", toFree, "but got", newPage)
	}
}
