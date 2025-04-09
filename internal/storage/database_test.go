package storage

import (
	"crypto/rand"
	"os"
	"testing"
)

func newDatabase(t *testing.T) *DatabaseManager {
	DatabaseManager := &DatabaseManager{}
	err := DatabaseManager.Initialize()
	if err != nil {
		t.Fatal("Failed to initialize database :", err)
	}

	err = DatabaseManager.wal.Initialize("test.log")
	if err != nil {
		t.Fatal("Failed to initialize database :", err)
	}

	err = DatabaseManager.allocator.Initialize("test.db")
	if err != nil {
		t.Fatal("Failed to initialize database :", err)
	}

	return DatabaseManager
}

func TestPageWriteAndRecovery(t *testing.T) {
	os.Remove("test.log")
	os.Remove("test.db")
	DatabaseManager := newDatabase(t)
	defer DatabaseManager.Shutdown()

	// allocate some pages
	PageCount := 5
	pageIDs := []uint64{}
	for i := 0; i < PageCount; i++ {
		pageID, err := DatabaseManager.allocator.AllocatePage(PagetypeUserdata)
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
		_, err := DatabaseManager.WritePages([]PageDelta{
			{
				id,
				0,
				data[:],
			},
		})
		if err != nil {
			t.Fatal("Write failed for page", id, ":", err)
		}
		pageData[id] = data
	}

	// Read back and verify the data
	for _, id := range pageIDs {
		readData, err := DatabaseManager.getPage(id)
		if err != nil {
			t.Fatal("Read failed for page", id, ":", err)
		}

		if string(readData[:]) != string(pageData[id][:]) {
			t.Error("Data mismatch for page", id)
		}
	}

	DatabaseManager.Shutdown()

	DatabaseManager = newDatabase(t)
	defer DatabaseManager.Shutdown()
	// try read back after a shutdown and restart
	for _, id := range pageIDs {
		readData, err := DatabaseManager.getPage(id)
		if err != nil {
			t.Fatal("Read failed for page", id, ":", err)
		}

		if string(readData[:]) != string(pageData[id][:]) {
			t.Error("Data mismatch for page", id)
		}
	}

	DatabaseManager.flushCheckpoint()
	// try read back after a checkpoint
	for _, id := range pageIDs {
		readData, err := DatabaseManager.getPage(id)
		if err != nil {
			t.Fatal("Read failed for page", id, ":", err)
		}

		if string(readData[:]) != string(pageData[id][:]) {
			t.Error("Data mismatch for page", id)
		}
	}

	DatabaseManager.Shutdown()

	DatabaseManager = newDatabase(t)
	defer DatabaseManager.Shutdown()
	// try read back after a checkpoint and shutdown
	for _, id := range pageIDs {
		readData, err := DatabaseManager.getPage(id)
		if err != nil {
			t.Fatal("Read failed for page", id, ":", err)
		}

		if string(readData[:]) != string(pageData[id][:]) {
			t.Error("Data mismatch for page", id)
		}
	}

}
