package storage

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

// WriteAheadLog implements the write-ahead logging mechanism for ensuring
// database durability and crash recovery. It maintains a log of all
// transactions and their changes to pages.
type WriteAheadLog struct {
	Log               *os.File                  // The log file handle
	FileName          string                    // Name of the log file
	Cache             map[uint64][]*Transaction // In-memory cache of transactions by page ID
	nextTransactionId uint64                    // Next transaction ID to assign
	fileSize          uint64                    // Current size of the log file
}

// Initialize sets up the WAL by opening the log file and recovering
// any existing transactions from disk. It validates transaction checksums
// and rebuilds the in-memory cache.
func (WriteAheadLog *WriteAheadLog) Initialize(fileName string) error {
	var err error
	WriteAheadLog.Log, err = os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	WriteAheadLog.FileName = fileName
	WriteAheadLog.refreshCache()

	// Read and validate existing transactions
	walReader := WalReader{}
	walReader.initialize(WriteAheadLog)
	offset := walReader.bytesRead
	for {
		offset = walReader.bytesRead
		transaction, err := walReader.getTransaction()
		if err != nil {
			// Truncate log at last valid transaction
			error := WriteAheadLog.Log.Truncate(int64(offset))
			if error != nil {
				return error
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		// Validate transaction checksum
		_, _, ok := transaction.checkSum()
		if !ok {
			continue
		}
		WriteAheadLog.addCache(transaction)
		WriteAheadLog.fileSize = walReader.bytesRead
	}
}

// refreshCache clears the in-memory transaction cache
func (WriteAheadLog *WriteAheadLog) refreshCache() {
	WriteAheadLog.Cache = make(map[uint64][]*Transaction)
}

// clearFromDisc removes the current log file and creates a new one.
// This is typically called after a successful checkpoint.
func (WriteAheadLog *WriteAheadLog) clearFromDisc() error {
	err := WriteAheadLog.closeFile()
	if err != nil {
		return err
	}
	err = os.Remove(WriteAheadLog.FileName)
	if err != nil {
		return err
	}
	err = WriteAheadLog.Initialize(WriteAheadLog.FileName)
	return err
}

// addCache adds a transaction to the in-memory cache, organizing
// it by the pages it modifies for efficient recovery
func (writeAheadLog *WriteAheadLog) addCache(transaction Transaction) {
	for _, body := range transaction.Body {
		if writeAheadLog.Cache[body.PageId] == nil {
			writeAheadLog.Cache[body.PageId] = make([]*Transaction, 0)
		}

		writeAheadLog.Cache[body.PageId] = append(writeAheadLog.Cache[body.PageId], &transaction)
	}
}

// AppendTransaction writes a new transaction to the log file.
// It includes:
// - Transaction ID
// - Number of pages modified
// - For each page: ID, offset, length, old data, new data
// - Transaction ID (repeated for validation)
// - Checksum
func (WriteAheadLog *WriteAheadLog) AppendTransaction(transaction Transaction) (error, uint64) {
	// Write transaction header
	data := binary.LittleEndian.AppendUint64([]byte{}, WriteAheadLog.nextTransactionId)
	data = binary.LittleEndian.AppendUint32(data, transaction.Header.pageCount)

	// Write each page modification
	for _, page := range transaction.Body {
		data = binary.LittleEndian.AppendUint64(data, page.PageId)
		data = binary.LittleEndian.AppendUint32(data, page.Offset)
		data = binary.LittleEndian.AppendUint32(data, page.Length)
		data = append(data, page.OldData...)
		data = append(data, page.NewData...)

		WriteAheadLog.addCache(transaction)
	}

	// Write transaction footer (ID and checksum)
	data = binary.LittleEndian.AppendUint64(data, WriteAheadLog.nextTransactionId)
	data = binary.LittleEndian.AppendUint32(data, getChecksumFromBytes(data))

	// Write to log file
	_, err := WriteAheadLog.Log.Write(data)
	if err != nil {
		return err, WriteAheadLog.nextTransactionId
	}

	WriteAheadLog.nextTransactionId++
	WriteAheadLog.fileSize += uint64(len(data))
	return nil, WriteAheadLog.nextTransactionId - 1
}

// closeFile closes the log file handle
func (WriteAheadLog *WriteAheadLog) closeFile() error {
	return WriteAheadLog.Log.Close()
}
