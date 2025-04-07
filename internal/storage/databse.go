package storage

import "fmt"

const (
	CHECKPOINT_SIZE_THRESHOLD = 10000
)

type DatabaseManager struct {
	database  map[uint64]PageData
	wal       WriteAheadLog
	allocator PageAllocator
	test      bool
}

type PageDelta struct {
	pageId  uint64
	offset  uint32
	oldData []byte
	newData []byte
}

func (databaseManager *DatabaseManager) Initialize() error {
	databaseManager.database = make(map[uint64]PageData)
	err := databaseManager.wal.Initialize("wal.log")
	if err != nil {
		return err
	}
	err = databaseManager.allocator.Initialize("data.db")
	return err
}

func (DatabaseManager *DatabaseManager) getPage(pageId uint64) (PageData, error) {
	data, ok := DatabaseManager.database[pageId]
	if ok {
		return data, nil
	}
	data, err := DatabaseManager.loadPageFromDisc(pageId)
	DatabaseManager.database[pageId] = data

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
		data, ok := DatabaseManager.database[pageDelta.pageId]
		if !ok {
			var err error
			data, err = DatabaseManager.loadPageFromDisc(pageDelta.pageId)
			DatabaseManager.database[pageDelta.pageId] = data
			if err != nil {
				return err, 0
			}
		}
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
		var ok bool
		data, ok = DatabaseManager.database[pageId]
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
	data, ok := DatabaseManager.database[change.pageId]
	if !ok {
		return fmt.Errorf("page not found in memory for page id %d", change.pageId)
	}
	// check for bounds
	end := int(change.offset) + len(change.newData)
	if end > len(data) {
		return fmt.Errorf("delta out of bounds on page %d", change.pageId)
	}
	// apply delta
	for i, b := range change.newData {
		DatabaseManager.database[change.pageId][change.offset+uint32(i)] = b
	}
	return nil
}

func (DatabaseManager *DatabaseManager) checkpointTrigger() error {
	if DatabaseManager.wal.fileSize >= CHECKPOINT_SIZE_THRESHOLD {
		return DatabaseManager.flushCheckpoint()
	}
	return nil
}
