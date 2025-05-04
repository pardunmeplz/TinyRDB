package storage

import (
	"encoding/binary"
	"hash/crc32"
)

// Transaction represents a complete database transaction in the WAL.
// It contains all changes made to pages during the transaction.
type Transaction struct {
	Header TransactionHeader // Transaction metadata
	Body   []PageEntry       // List of page changes
	End    TransactionEnd    // Transaction footer with validation
}

// MakeTransaction initializes a new transaction with an empty page change list
func (Transaction *Transaction) MakeTransaction() *Transaction {
	Transaction.Body = make([]PageEntry, 0)
	return Transaction
}

// checkSum calculates and verifies the transaction checksum.
// The checksum covers:
// - Transaction ID
// - Number of page changes
// - All page changes (ID, offset, length, old data, new data)
// - Transaction ID (repeated)
// Returns:
// - Calculated checksum
// - Stored checksum
// - Whether they match
func (transaction *Transaction) checkSum() (uint32, uint32, bool) {
	// Build data for checksum calculation
	data := binary.LittleEndian.AppendUint64([]byte{}, transaction.Header.transactionId)
	data = binary.LittleEndian.AppendUint32(data, transaction.Header.pageCount)

	// Add all page changes
	for _, page := range transaction.Body {
		data = binary.LittleEndian.AppendUint64(data, page.PageId)
		data = binary.LittleEndian.AppendUint32(data, page.Offset)
		data = binary.LittleEndian.AppendUint32(data, page.Length)
		data = append(data, page.OldData...)
		data = append(data, page.NewData...)
	}

	// Add transaction ID again for validation
	data = binary.LittleEndian.AppendUint64(data, transaction.Header.transactionId)
	checksum := getChecksumFromBytes(data)
	return checksum, transaction.End.Checksum, transaction.End.Checksum == checksum
}

// TransactionHeader contains metadata about a transaction
type TransactionHeader struct {
	transactionId uint64 // Unique identifier for the transaction
	pageCount     uint32 // Number of pages modified in this transaction
}

// PageEntry represents a single change to a page in a transaction.
// It contains both the old and new data to support rollback.
type PageEntry struct {
	PageId  uint64 // ID of the modified page
	Offset  uint32 // Starting offset in the page
	Length  uint32 // Length of the change
	OldData []byte // Original data before the change
	NewData []byte // New data after the change
}

// TransactionEnd contains validation information for the transaction.
// The transaction ID is repeated here to detect truncation.
type TransactionEnd struct {
	TransactionId uint64 // Transaction ID (repeated for validation)
	Checksum      uint32 // CRC32 checksum of the entire transaction
}

// getChecksumFromBytes calculates a CRC32 checksum for a byte slice
func getChecksumFromBytes(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
