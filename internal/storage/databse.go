package storage

import "fmt"

const (
	CHECKPOINT_SIZE_THRESHOLD = 10000
	CACHE_CAPACITY_PAGES      = 32000
)

type DatabaseManager struct {
	database  map[uint64]*CacheEntry
	head      *CacheEntry
	tail      *CacheEntry
	wal       WriteAheadLog
	allocator PageAllocator
	test      bool
}

type CacheEntry struct {
	data PageData
	next *CacheEntry
	prev *CacheEntry
}

type PageDelta struct {
	pageId  uint64
	offset  uint32
	oldData []byte
	newData []byte
}

func (databaseManager *DatabaseManager) Initialize() error {
	databaseManager.database = make(map[uint64]*CacheEntry)
	err := databaseManager.wal.Initialize("wal.log")
	if err != nil {
		return err
	}
	err = databaseManager.allocator.Initialize("data.db")
	return err
}

func (DatabaseManager *DatabaseManager) getPage(pageId uint64) (PageData, error) {
	entry, ok := DatabaseManager.database[pageId]
	if ok {
		DatabaseManager.makeHead(pageId)
		return entry.data, nil
	}
	data, err := DatabaseManager.loadPageFromDisc(pageId)
	DatabaseManager.addCacheData(data, pageId)

	return data, err
}

func (DatabaseManager *DatabaseManager) writePages(changes []PageDelta) (error, uint64) {
	// checkpoint
	err := DatabaseManager.checkpointTrigger()
	if err != nil {
		return err, 0
	}

	// make transaction
	transaction := Transaction{}
	transaction.MakeTransaction()
	transaction.Header.pageCount = uint32(len(changes))
	for _, pageDelta := range changes {
		// load page
		entry, ok := DatabaseManager.database[pageDelta.pageId]
		if !ok {
			var err error
			data, err := DatabaseManager.loadPageFromDisc(pageDelta.pageId)
			DatabaseManager.addCacheData(data, pageDelta.pageId)
			entry.data = data
			if err != nil {
				return err, 0
			}
		} else {
			DatabaseManager.makeHead(pageDelta.pageId)
		}
		data := entry.data

		// add delta to body
		body := PageEntry{}
		body.PageId = pageDelta.pageId
		body.Offset = pageDelta.offset
		body.Length = uint32(len(pageDelta.newData))
		body.NewData = pageDelta.newData

		end := int(pageDelta.offset) + len(pageDelta.newData)
		if end > len(data) {
			return fmt.Errorf("delta out of bounds on page %d", pageDelta.pageId), 0
		}
		body.OldData = data[pageDelta.offset : body.Length+pageDelta.offset]
		transaction.Body = append(transaction.Body, body)
	}

	for _, pageDelta := range changes {
		DatabaseManager.applyDelta(pageDelta)
	}
	err, transactionId := DatabaseManager.wal.AppendTransaction(transaction)

	return err, transactionId
}

func (DatabaseManager *DatabaseManager) Shutdown() {
	DatabaseManager.wal.closeFile()
	DatabaseManager.allocator.CloseFile()
}

func (DatabaseManager *DatabaseManager) loadPageFromDisc(pageId uint64) (PageData, error) {

	data, err := DatabaseManager.allocator.ReadPageData(pageId)
	if err != nil {
		return data, err
	}

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
	if DatabaseManager.wal.fileSize >= CHECKPOINT_SIZE_THRESHOLD {
		return DatabaseManager.flushCheckpoint()
	}
	return nil
}

func (DatabaseManager *DatabaseManager) addCacheData(data PageData, pageId uint64) {
	if len(DatabaseManager.database) >= CACHE_CAPACITY_PAGES {
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

	if tail.next == nil {
		DatabaseManager.tail = tail.next
		DatabaseManager.tail.prev = nil
	} else {
		DatabaseManager.head = nil
		DatabaseManager.tail = nil
	}

}
