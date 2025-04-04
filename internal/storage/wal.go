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

func (WriteAheadLog *WriteAheadLog) Initialize(fileName string) error {
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

func (WriteAheadLog *WriteAheadLog) AppendTransaction(transaction Transaction) error {
	data := binary.LittleEndian.AppendUint64([]byte{}, WriteAheadLog.nextTransactionId)
	data = binary.LittleEndian.AppendUint32(data, transaction.Header.pageCount)

	for _, page := range transaction.Body {
		data = binary.LittleEndian.AppendUint64(data, page.PageId)
		data = binary.LittleEndian.AppendUint32(data, page.Offset)
		data = binary.LittleEndian.AppendUint32(data, page.Length)
		data = append(data, page.OldData...)
		data = append(data, page.NewData...)

		WriteAheadLog.addCache(page.PageId, page.Offset, page.Length, WriteAheadLog.nextTransactionId, page.NewData)
	}

	data = binary.LittleEndian.AppendUint64(data, WriteAheadLog.nextTransactionId)
	data = append(data, transaction.End.Status)
	data = binary.LittleEndian.AppendUint32(data, getChecksumFromBytes(data))

	_, err := WriteAheadLog.log.Write(data)
	if err != nil {
		return err
	}

	WriteAheadLog.nextTransactionId++
	return nil
}

func (WriteAheadLog *WriteAheadLog) closeFile() {
	WriteAheadLog.log.Close()
}
