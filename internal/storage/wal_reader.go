package storage

import (
	"bufio"
	"encoding/binary"
	"io"
)

// WalReader handles reading transactions from the Write-Ahead Log.
// It maintains state about the current read position and provides
// methods to parse transaction records from the log file.
type WalReader struct {
	WriteAheadLog *WriteAheadLog // Reference to the WAL being read
	reader        io.Reader      // Buffered reader for the log file
	bytesRead     uint64         // Total bytes read from the log
}

// Startup initializes the WAL reader and verifies the first transaction
// can be read successfully. This is used during database startup to
// validate the WAL file.
func (WriteAheadLog *WriteAheadLog) Startup() error {
	WalReader := WalReader{}
	WalReader.initialize(WriteAheadLog)
	_, err := WalReader.getTransaction()
	if err != nil {
		return err
	}
	return nil
}

// initialize sets up the WAL reader with a buffered reader and resets
// the read position to the start of the file.
func (WalReader *WalReader) initialize(WriteAheadLog *WriteAheadLog) {
	WalReader.reader = bufio.NewReader(WriteAheadLog.Log)
	WalReader.WriteAheadLog = WriteAheadLog
	WriteAheadLog.Log.Seek(0, io.SeekStart)
	WalReader.bytesRead = 0
}

// getTransaction reads and parses a complete transaction record from the WAL.
// The transaction format is:
// - Transaction ID (uint64)
// - Number of page changes (uint32)
// - For each page change:
//   - Page ID (uint64)
//   - Offset in page (uint32)
//   - Length of change (uint32)
//   - Old data (byte array)
//   - New data (byte array)
//
// - Transaction ID (repeated for validation)
// - Checksum (uint32)
func (WalReader *WalReader) getTransaction() (Transaction, error) {
	transaction := Transaction{}
	transaction.MakeTransaction()

	// Read transaction header
	err := binary.Read(WalReader.reader, binary.LittleEndian, &transaction.Header.transactionId)
	if err != nil {
		return transaction, err
	}
	WalReader.bytesRead += uint64(binary.Size(transaction.Header.transactionId))

	err = binary.Read(WalReader.reader, binary.LittleEndian, &transaction.Header.pageCount)
	if err != nil {
		return transaction, err
	}
	WalReader.bytesRead += uint64(binary.Size(transaction.Header.pageCount))

	// Read each page change in the transaction
	for range transaction.Header.pageCount {
		body := PageEntry{}

		// Read page change metadata
		err = binary.Read(WalReader.reader, binary.LittleEndian, &body.PageId)
		if err != nil {
			return transaction, err
		}
		WalReader.bytesRead += uint64(binary.Size(body.PageId))

		err = binary.Read(WalReader.reader, binary.LittleEndian, &body.Offset)
		if err != nil {
			return transaction, err
		}
		WalReader.bytesRead += uint64(binary.Size(body.Offset))

		err = binary.Read(WalReader.reader, binary.LittleEndian, &body.Length)
		if err != nil {
			return transaction, err
		}
		WalReader.bytesRead += uint64(binary.Size(body.Length))

		// Read old and new data
		body.OldData = make([]byte, body.Length)
		err = binary.Read(WalReader.reader, binary.LittleEndian, body.OldData)
		if err != nil {
			return transaction, err
		}
		WalReader.bytesRead += uint64(body.Length)

		body.NewData = make([]byte, body.Length)
		err = binary.Read(WalReader.reader, binary.LittleEndian, body.NewData)
		if err != nil {
			return transaction, err
		}
		WalReader.bytesRead += uint64(body.Length)
		transaction.Body = append(transaction.Body, body)
	}

	// Read transaction footer (ID and checksum)
	err = binary.Read(WalReader.reader, binary.LittleEndian, &transaction.End.TransactionId)
	if err != nil {
		return transaction, err
	}
	WalReader.bytesRead += uint64(binary.Size(transaction.End.TransactionId))

	err = binary.Read(WalReader.reader, binary.LittleEndian, &transaction.End.Checksum)
	if err != nil {
		return transaction, err
	}
	WalReader.bytesRead += uint64(binary.Size(transaction.End.Checksum))

	return transaction, nil
}
