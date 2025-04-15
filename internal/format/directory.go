package format

import (
	s "relationalDatabase/internal/storage"
)

type DirectoryEntry struct {
	TableNameLen byte
	TableName    string
	PageId       uint64
}

type Directory struct {
	schemas  map[string]Schema
	database s.DatabaseManager
}

func (directory *Directory) initializeDirectory(database s.DatabaseManager) {
	directory.database = database
	data, err := database.GetPage(1)
}

func (directory Directory) addEntry(DirectoryEntry) {

}
