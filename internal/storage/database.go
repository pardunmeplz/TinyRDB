package storage

import "fmt"

//CHECKPOINT_SIZE_THRESHOLD = 10000
//CACHE_CAPACITY_PAGES      = 32000

// DatabaseManager handles the core database operations including page management,
// caching, and transaction handling. It implements ACID compliance through
// write-ahead logging and checkpointing.
type DatabaseManager struct {
	// database maps page IDs to their cache entries
	database map[uint64]*CacheEntry
	// head and tail maintain an LRU cache of pages
	head *CacheEntry
	tail *CacheEntry
	// wal handles write-ahead logging for durability
	wal WriteAheadLog
	// allocator manages page allocation and deallocation
	allocator PageAllocator
	// test flag for testing purposes
	test bool
	// cacheCapacityPages limits the number of pages in memory
	cacheCapacityPages int
	// checkpointSizeThreshold triggers checkpoint when WAL reaches this size
	checkpointSizeThreshold uint64
}

// CacheEntry represents a page in the LRU cache
type CacheEntry struct {
	data PageData
	next *CacheEntry
	prev *CacheEntry
}

// PageDelta represents a change to be made to a page
type PageDelta struct {
	pageId  uint64 // ID of the page to modify
	offset  uint32 // Starting offset in the page
	newData []byte // New data to write
}

// Initialize sets up the database manager with specified cache and checkpoint parameters
func (databaseManager *DatabaseManager) Initialize(checkpointTresholdInBytes uint64, cacheCapacityInPages int) error {
	databaseManager.database = make(map[uint64]*CacheEntry)
	err := databaseManager.wal.Initialize("wal.log")
	if err != nil {
		return err
	}
	err = databaseManager.allocator.Initialize("data.db")
	databaseManager.cacheCapacityPages = cacheCapacityInPages
	databaseManager.checkpointSizeThreshold = checkpointTresholdInBytes
	return err
}

// AllocatePage allocates a new page of the specified type
func (DatabaseManager *DatabaseManager) AllocatePage(pageType byte) (uint64, error) {
	return DatabaseManager.allocator.AllocatePage(pageType)
}

// GetPage retrieves a page from cache or disk, applying any pending WAL changes
func (DatabaseManager *DatabaseManager) GetPage(pageId uint64) (PageData, error) {
	entry, ok := DatabaseManager.database[pageId]
	if ok {
		DatabaseManager.makeHead(pageId)
		return entry.data, nil
	}
	data, err := DatabaseManager.loadPageFromDisc(pageId)
	DatabaseManager.addCacheData(data, pageId)

	return data, err
}

// WritePages applies a set of changes to pages, ensuring ACID compliance
// through WAL logging and checkpointing
func (DatabaseManager *DatabaseManager) WritePages(changes []PageDelta) (uint64, error) {
	// Check if we need to perform a checkpoint
	err := DatabaseManager.checkpointTrigger()
	if err != nil {
		return 0, err
	}

	// Create a new transaction
	transaction := Transaction{}
	transaction.MakeTransaction()
	transaction.Header.pageCount = uint32(len(changes))

	// Process each page change
	for _, pageDelta := range changes {
		// Load the page from cache or disk
		entry, ok := DatabaseManager.database[pageDelta.pageId]
		var data PageData
		if !ok {
			var err error
			discData, err := DatabaseManager.loadPageFromDisc(pageDelta.pageId)
			DatabaseManager.addCacheData(discData, pageDelta.pageId)
			data = discData
			if err != nil {
				return 0, err
			}
		} else {
			DatabaseManager.makeHead(pageDelta.pageId)
			data = entry.data
		}

		// Create WAL entry for the change
		body := PageEntry{}
		body.PageId = pageDelta.pageId
		body.Offset = pageDelta.offset
		body.Length = uint32(len(pageDelta.newData))
		body.NewData = pageDelta.newData

		// Validate the change is within page bounds
		end := int(pageDelta.offset) + len(pageDelta.newData)
		if end > len(data) {
			return 0, fmt.Errorf("delta out of bounds on page %d", pageDelta.pageId)
		}
		body.OldData = data[pageDelta.offset : body.Length+pageDelta.offset]
		transaction.Body = append(transaction.Body, body)
	}

	// Apply changes to pages
	for _, pageDelta := range changes {
		DatabaseManager.applyDelta(pageDelta)
	}

	// Log the transaction to WAL
	err, transactionId := DatabaseManager.wal.AppendTransaction(transaction)

	return transactionId, err
}

func (DatabaseManager *DatabaseManager) Shutdown() {
	DatabaseManager.wal.closeFile()
	DatabaseManager.allocator.CloseFile()
}

// loadPageFromDisc loads a page from disk and applies any pending WAL changes
func (DatabaseManager *DatabaseManager) loadPageFromDisc(pageId uint64) (PageData, error) {
	data, err := DatabaseManager.allocator.ReadPageData(pageId)
	if err != nil {
		return data, err
	}

	// Apply any pending WAL changes to the page
	walEntries, ok := DatabaseManager.wal.Cache[pageId]
	if ok {
		for _, e := range walEntries {
			for _, body := range e.Body {
				if body.PageId != pageId {
					continue
				}
				for i, b := range body.NewData {
					data[body.Offset+uint32(i)] = b
				}
			}
		}
	}

	return data, nil
}

// flushCheckpoint writes all dirty pages to disk and clears the WAL
func (DatabaseManager *DatabaseManager) flushCheckpoint() error {
	var data PageData
	for pageId := range DatabaseManager.wal.Cache {
		entry, ok := DatabaseManager.database[pageId]
		data = entry.data
		if !ok {
			var err error
			data, err = DatabaseManager.loadPageFromDisc(pageId)
			if err != nil {
				return err
			}
		}
		err := DatabaseManager.allocator.WritePageData(pageId, data)
		if err != nil {
			return err
		}
	}
	err := DatabaseManager.wal.clearFromDisc()
	return err
}

func (DatabaseManager *DatabaseManager) applyDelta(change PageDelta) error {
	// check if page exists
	entry, ok := DatabaseManager.database[change.pageId]
	if !ok {
		return fmt.Errorf("page not found in memory for page id %d", change.pageId)
	}
	data := entry.data
	// check for bounds
	end := int(change.offset) + len(change.newData)
	if end > len(data) {
		return fmt.Errorf("delta out of bounds on page %d", change.pageId)
	}
	// apply delta
	for i, b := range change.newData {
		DatabaseManager.database[change.pageId].data[change.offset+uint32(i)] = b
	}
	return nil
}

func (DatabaseManager *DatabaseManager) checkpointTrigger() error {
	if DatabaseManager.wal.fileSize >= DatabaseManager.checkpointSizeThreshold {
		return DatabaseManager.flushCheckpoint()
	}
	return nil
}

func (DatabaseManager *DatabaseManager) addCacheData(data PageData, pageId uint64) {
	if len(DatabaseManager.database) >= DatabaseManager.cacheCapacityPages {
		DatabaseManager.removeTail()
	}
	newEntry := CacheEntry{data, nil, DatabaseManager.head}
	if DatabaseManager.head != nil {
		DatabaseManager.head.next = &newEntry
	} else {
		DatabaseManager.tail = &newEntry
	}
	DatabaseManager.database[pageId] = &newEntry
	DatabaseManager.head = &newEntry

}

func (DatabaseManager *DatabaseManager) makeHead(pageId uint64) {
	if DatabaseManager.database[pageId].next != nil {
		DatabaseManager.database[pageId].next.prev = DatabaseManager.database[pageId].prev
	}
	if DatabaseManager.database[pageId].prev != nil {
		DatabaseManager.database[pageId].prev.next = DatabaseManager.database[pageId].next
	}
	DatabaseManager.database[pageId].prev = DatabaseManager.head
	DatabaseManager.database[pageId].next = nil
	DatabaseManager.head = DatabaseManager.database[pageId]
}

func (DatabaseManager *DatabaseManager) removeTail() {
	tail := DatabaseManager.tail
	if tail == nil {
		return
	}

	for pageId, entry := range DatabaseManager.database {
		if tail == entry {
			delete(DatabaseManager.database, pageId)
			break
		}
	}

	if tail.next != nil {
		DatabaseManager.tail = tail.next
		DatabaseManager.tail.prev = nil
	} else {
		DatabaseManager.head = nil
		DatabaseManager.tail = nil
	}

}
