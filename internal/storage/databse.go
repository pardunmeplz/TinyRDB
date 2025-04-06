package storage

type DatabaseManager struct {
	database  map[uint64]PageData
	wal       WriteAheadLog
	allocator PageAllocator
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

	DatabaseManager.database[pageId] = data

	return data, nil
}

func (DatabaseManager *DatabaseManager) Shutdown() {
	DatabaseManager.wal.closeFile()
	DatabaseManager.allocator.CloseFile()
}
