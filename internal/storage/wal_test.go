package storage

import (
	"encoding/binary"
	"os"
	"reflect"
	"testing"
)

func newWal(t *testing.T) *WriteAheadLog {

	writeAheadLog := &WriteAheadLog{}
	err := writeAheadLog.Initialize("wal.log")
	if err != nil {
		t.Fatal("Failed to initialize wal :", err)
	}
	return writeAheadLog
}

func TestAppendRead(t *testing.T) {
	os.Remove("wal.log")
	wal := newWal(t)
	defer wal.closeFile()

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

	if readTransaction.Header.pageCount != transaction.Header.pageCount ||
		readTransaction.Body[0].Length != transaction.Body[0].Length ||
		readTransaction.Body[0].Offset != transaction.Body[0].Offset ||
		readTransaction.Body[0].PageId != transaction.Body[0].PageId ||
		!reflect.DeepEqual(readTransaction.Body[0].NewData, transaction.Body[0].NewData) ||
		!reflect.DeepEqual(readTransaction.Body[0].OldData, transaction.Body[0].OldData) {

		t.Fatal("Value mismatch, cache transaction is not equal to written transaction ")
	}

}

func TestReadingAtStartup(t *testing.T) {
	os.Remove("wal.log")
	wal := newWal(t)
	defer wal.closeFile()

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
	transaction.End.Checksum = 0 // will be overwritten in append

	err := wal.AppendTransaction(transaction)
	if err != nil {
		t.Fatal("Failed to write transaction: ", err)
	}
	wal.Log.Sync()
	wal.closeFile()

	walNew := newWal(t)
	defer walNew.closeFile()

	cacheTransaction := walNew.Cache[42][0]
	checksum, checksumnew, ok := cacheTransaction.checkSum()
	if !ok {
		t.Fatal("Failed checksum for transaction ", checksum, checksumnew)
	}
	if cacheTransaction.Header.pageCount != transaction.Header.pageCount ||
		cacheTransaction.Body[0].Length != transaction.Body[0].Length ||
		cacheTransaction.Body[0].Offset != transaction.Body[0].Offset ||
		cacheTransaction.Body[0].PageId != transaction.Body[0].PageId ||
		!reflect.DeepEqual(cacheTransaction.Body[0].NewData, transaction.Body[0].NewData) ||
		!reflect.DeepEqual(cacheTransaction.Body[0].OldData, transaction.Body[0].OldData) {

		t.Fatal("Value mismatch, cache transaction is not equal to written transaction ")
	}

}

func TestTruncate(t *testing.T) {
	os.Remove("wal.log")
	wal := newWal(t)
	defer wal.closeFile()

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
	transaction.End.Checksum = 0 // will be overwritten in append

	err := wal.AppendTransaction(transaction)
	if err != nil {
		t.Fatal("Failed to write transaction: ", err)
	}

	// duplicate entry with checksum mismatch
	err = wal.AppendTransaction(transaction)
	if err != nil {
		t.Fatal("Failed to write transaction: ", err)
	}
	// override the checksum
	wal.Log.Seek(-4, 1)
	wal.Log.Write([]byte{0, 1, 1, 0})

	// put in an incomplete transaction
	data := []byte{}
	data = binary.LittleEndian.AppendUint64(data, transaction.Header.transactionId) // transactionId
	data = binary.LittleEndian.AppendUint32(data, 1)                                // page count
	data = binary.LittleEndian.AppendUint64(data, 43)                               // page id
	wal.Log.Write(data)

	wal.Log.Sync()
	preInfo, err := wal.Log.Stat()
	if err != nil {
		t.Fatal("Failed to get pre-file size: ", err)
	}
	preSize := preInfo.Size()
	wal.closeFile()

	walNew := newWal(t)
	defer walNew.closeFile()
	postInfo, err := walNew.Log.Stat()
	if err != nil {
		t.Fatal("Failed to get post-file size: ", err)
	}
	postSize := postInfo.Size()
	difference := int64(len(data))

	if preSize-postSize != difference {
		t.Fatal("Expected truncation size was ", len(data), " instead got ", preInfo.Size()-postInfo.Size())
	}
	if len(walNew.Cache) != 1 {
		t.Fatal("Expected 1 transaction in cache after recovery, got", len(walNew.Cache))
	}

}
