package storage

import (
	"os"
	"testing"
)

func newWal(t *testing.T) *WriteAheadLog {
	os.Remove("wal.log")

	writeAheadLog := &WriteAheadLog{}
	err := writeAheadLog.Initialize("wal.log")
	if err != nil {
		t.Fatal("Failed to initialize wal :", err)
	}
	return writeAheadLog
}

func TestAppendRead(t *testing.T) {
	wal := newWal(t)

	// --- Create a dummy transaction ---
	transaction := Transaction{}
	transaction.MakeTransaction()

	transaction.Header.pageCount = 1
	page := PageEntry{
		PageId:  42,
		Offset:  10,
		Length:  4,
		OldData: []byte{1, 2, 3, 4},
		NewData: []byte{5, 6, 7, 8},
	}
	transaction.Body = append(transaction.Body, page)
	transaction.End.TransactionId = 1
	transaction.End.Status = 1   // committed
	transaction.End.Checksum = 0 // will be overwritten in append

	err := wal.AppendTransaction(transaction)
	if err != nil {
		t.Fatal("Failed to write transaction: ", err)
	}
	wal.Log.Sync()

	walReader := WalReader{}
	walReader.initialize(wal)

	readTransaction, err := walReader.getTransaction()
	if err != nil {
		t.Fatal("Failed to read transaction :", err, transaction, readTransaction)
	}
	checksum, checksumnew, ok := readTransaction.checkSum()

	if !ok {
		t.Fatal("Failed checksum for transaction ", checksum, checksumnew)
	}

}
