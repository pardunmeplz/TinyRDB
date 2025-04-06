package storage

import (
	"encoding/binary"
	"hash/crc32"
)

type Transaction struct {
	Header TransactionHeader
	Body   []PageEntry
	End    TransactionEnd
}

func (Transaction *Transaction) MakeTransaction() *Transaction {
	Transaction.Body = make([]PageEntry, 0)
	return Transaction
}

func (transaction *Transaction) checkSum() (uint32, uint32, bool) {
	data := binary.LittleEndian.AppendUint64([]byte{}, transaction.Header.transactionId)
	data = binary.LittleEndian.AppendUint32(data, transaction.Header.pageCount)

	for _, page := range transaction.Body {
		data = binary.LittleEndian.AppendUint64(data, page.PageId)
		data = binary.LittleEndian.AppendUint32(data, page.Offset)
		data = binary.LittleEndian.AppendUint32(data, page.Length)
		data = append(data, page.OldData...)
		data = append(data, page.NewData...)
	}

	data = binary.LittleEndian.AppendUint64(data, transaction.Header.transactionId)
	checksum := getChecksumFromBytes(data)
	return checksum, transaction.End.Checksum, transaction.End.Checksum == checksum
}

type TransactionHeader struct {
	transactionId uint64
	pageCount     uint32
}

type PageEntry struct {
	PageId  uint64
	Offset  uint32
	Length  uint32
	OldData []byte
	NewData []byte
}

type TransactionEnd struct {
	TransactionId uint64
	Checksum      uint32
}

func getChecksumFromBytes(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
