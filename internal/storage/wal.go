package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

type WriteAheadLog struct {
	Log               *os.File
	Cache             map[uint64][]Transaction
	nextTransactionId uint64
}

func (WriteAheadLog *WriteAheadLog) Initialize(fileName string) error {
	var err error
	WriteAheadLog.Log, err = os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	WriteAheadLog.refreshCache()

	walReader := WalReader{}
	walReader.initialize(WriteAheadLog)
	for {
		transaction, err := walReader.getTransaction()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		WriteAheadLog.addCache(transaction)
	}
}

func (WriteAheadLog *WriteAheadLog) refreshCache() {
	WriteAheadLog.Cache = make(map[uint64][]Transaction)
}

func (writeAheadLog *WriteAheadLog) addCache(transaction Transaction) {
	for _, body := range transaction.Body {
		if writeAheadLog.Cache[body.PageId] == nil {
			writeAheadLog.Cache[body.PageId] = make([]Transaction, 0)
		}

		writeAheadLog.Cache[body.PageId] = append(writeAheadLog.Cache[body.PageId], transaction)
	}
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

		WriteAheadLog.addCache(transaction)
	}

	data = binary.LittleEndian.AppendUint64(data, WriteAheadLog.nextTransactionId)
	data = append(data, transaction.End.Status)
	data = binary.LittleEndian.AppendUint32(data, getChecksumFromBytes(data))
	fmt.Println(data)

	_, err := WriteAheadLog.Log.Write(data)
	if err != nil {
		return err
	}

	WriteAheadLog.nextTransactionId++
	return nil
}

func (WriteAheadLog *WriteAheadLog) closeFile() {
	WriteAheadLog.Log.Close()
}
