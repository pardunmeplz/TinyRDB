package storage

import (
	"encoding/binary"
	"os"
)

type WriteAheadLog struct {
	log               *os.File
	cache             map[uint64][]WalIndexRecord
	nextTransactionId uint64
}

func (WriteAheadLog *WriteAheadLog) initialize(fileName string) error {
	var err error
	WriteAheadLog.log, err = os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}

	return err
}

func (WriteAheadLog *WriteAheadLog) refreshCache() {
	WriteAheadLog.cache = make(map[uint64][]WalIndexRecord)
}

func (writeAheadLog *WriteAheadLog) addCache(page uint64, offset uint32, size uint32, transaction uint64, data []byte) {
	if writeAheadLog.cache[page] == nil {
		writeAheadLog.cache[page] = make([]WalIndexRecord, 0)
	}

	writeAheadLog.cache[page] = append(writeAheadLog.cache[page], WalIndexRecord{offset, size, transaction, data})
}

func (WriteAheadLog *WriteAheadLog) AppendTransaction(transaction Transaction) {
	data := binary.LittleEndian.AppendUint64([]byte{}, WriteAheadLog.nextTransactionId)
	data = binary.LittleEndian.AppendUint32(data, transaction.header.pageCount)

	for _, page := range transaction.body {
		data = binary.LittleEndian.AppendUint64(data, page.pageId)
		data = binary.LittleEndian.AppendUint32(data, page.offset)
		data = binary.LittleEndian.AppendUint32(data, page.length)
		data = append(data, page.oldData...)
		data = append(data, page.newData...)

		WriteAheadLog.addCache(page.pageId, page.offset, page.length, WriteAheadLog.nextTransactionId, page.newData)
	}

	data = binary.LittleEndian.AppendUint64(data, WriteAheadLog.nextTransactionId)
	data = append(data, transaction.end.status)
	data = binary.LittleEndian.AppendUint32(data, getChecksumFromBytes(data))

	WriteAheadLog.log.Write(data)
	WriteAheadLog.nextTransactionId++
}

func (WriteAheadLog *WriteAheadLog) closeFile() {
	WriteAheadLog.log.Close()
}
