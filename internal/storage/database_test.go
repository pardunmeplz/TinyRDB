package storage

import (
	"crypto/rand"
	"os"
	"testing"
)

func newDatabase(t *testing.T, checkPointTrigger uint64, cacheSize int) *DatabaseManager {
	DatabaseManager := &DatabaseManager{}
	err := DatabaseManager.Initialize(checkPointTrigger, cacheSize)
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
	DatabaseManager := newDatabase(t, 10000, 32000)
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

	{
		// rewrite on last page to test durability on multiple writes for same page
		data := MakePageData()
		rand.Read(data[:])
		_, err := DatabaseManager.WritePages([]PageDelta{
			{
				pageIDs[4],
				0,
				data[:],
			},
		})
		if err != nil {
			t.Fatal("Write failed for page", pageIDs[4], ":", err)
		}
		pageData[pageIDs[4]] = data
	}

	{
		// partial rewrite on 4th page to test partial writes
		data := make([]byte, 15)
		rand.Read(data[:])
		_, err := DatabaseManager.WritePages([]PageDelta{
			{
				pageIDs[3],
				50,
				data[:],
			},
		})
		if err != nil {
			t.Fatal("Write failed for page", pageIDs[3], ":", err)
		}
		for i, byteData := range data {
			pageData[pageIDs[3]][50+i] = byteData
		}
	}

	// Read back and verify the data
	for _, id := range pageIDs {
		readData, err := DatabaseManager.GetPage(id)
		if err != nil {
			t.Fatal("Read failed for page", id, ":", err)
		}

		if string(readData[:]) != string(pageData[id][:]) {
			t.Error("Data mismatch for page", id)
		}
	}

	DatabaseManager.Shutdown()

	DatabaseManager = newDatabase(t, 10000, 32000)
	defer DatabaseManager.Shutdown()
	// try read back after a shutdown and restart
	for _, id := range pageIDs {
		readData, err := DatabaseManager.GetPage(id)
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
		readData, err := DatabaseManager.GetPage(id)
		if err != nil {
			t.Fatal("Read failed for page", id, ":", err)
		}

		if string(readData[:]) != string(pageData[id][:]) {
			t.Error("Data mismatch for page", id)
		}
	}

	DatabaseManager.Shutdown()

	DatabaseManager = newDatabase(t, 10000, 32000)
	defer DatabaseManager.Shutdown()
	// try read back after a checkpoint and shutdown
	for _, id := range pageIDs {
		readData, err := DatabaseManager.GetPage(id)
		if err != nil {
			t.Fatal("Read failed for page", id, ":", err)
		}

		if string(readData[:]) != string(pageData[id][:]) {
			t.Error("Data mismatch for page", id)
		}
	}

}

func TestCacheEviction(t *testing.T) {
	os.Remove("test.log")
	os.Remove("test.db")
	DatabaseManager := newDatabase(t, 10000, 3)
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

	readData, ok := DatabaseManager.database[pageIDs[4]]

	if !ok {
		t.Fatal("Page 4 was not retained in cache")
	}

	if string(readData.data[:]) != string(pageData[pageIDs[4]][:]) {
		t.Error("Data mismatch for page", pageData[pageIDs[4]])
	}

	readData, ok = DatabaseManager.database[pageIDs[0]]
	if ok {
		t.Fatal("Page 0 was not removed from cache")
	}

	readPage, err := DatabaseManager.GetPage(pageIDs[0])
	if err != nil {
		t.Fatal("Failed to read page ", err)
	}
	if string(readPage[:]) != string(pageData[pageIDs[0]][:]) {
		t.Error("Data mismatch for page", pageData[pageIDs[0]])
	}
}

func TestCheckpointTrigger(t *testing.T) {
	os.Remove("test.log")
	os.Remove("test.db")
	checkpointTrigger := 10000
	DatabaseManager := newDatabase(t, uint64(checkpointTrigger), 32000)
	defer DatabaseManager.Shutdown()

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

	stats, err := DatabaseManager.wal.Log.Stat()
	if err != nil {
		t.Fatal("Page Stat failed :", err)
	}
	walSize := stats.Size()
	t.Log(walSize)
	if walSize > int64(checkpointTrigger) {
		t.Fatal("Wal Truncation failed :", err)
	}

	readData, err := DatabaseManager.allocator.ReadPageData(pageIDs[0])
	if err != nil {
		t.Fatal("Page read failed  :", err)
	}

	if string(readData[:]) != string(pageData[pageIDs[0]][:]) {
		t.Error("Data mismatch during transfer to disk at page ", pageIDs[0])
	}

}
