package storage

import "hash/crc32"

type Transaction struct {
	header TransactionHeader
	body   []PageEntry
	end    TransactionEnd
}

type TransactionHeader struct {
	transactionId uint64
	pageCount     uint32
}

type PageEntry struct {
	pageId  uint64
	offset  uint32
	length  uint32
	oldData []byte
	newData []byte
}

type TransactionEnd struct {
	transactionId uint64
	status        byte
	cheksum       uint32
}

func getChecksumFromBytes(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

const (
	COMMIT   = 0
	ROLLBACK = 1
)

type WalIndexRecord struct {
	offset      uint32
	size        uint32
	transaction uint64
	newData     []byte
}
