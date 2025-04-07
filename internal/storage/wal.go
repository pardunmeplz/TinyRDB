package storage

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

type WriteAheadLog struct {
	Log               *os.File
	FileName          string
	Cache             map[uint64][]*Transaction
	nextTransactionId uint64
	fileSize          uint64
}

func (WriteAheadLog *WriteAheadLog) Initialize(fileName string) error {
	var err error
	WriteAheadLog.Log, err = os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	WriteAheadLog.FileName = fileName
	WriteAheadLog.refreshCache()

	walReader := WalReader{}
	walReader.initialize(WriteAheadLog)
	offset := walReader.bytesRead
	for {
		offset = walReader.bytesRead
		transaction, err := walReader.getTransaction()
		if err != nil {
			error := WriteAheadLog.Log.Truncate(int64(offset))
			if error != nil {
				return error
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		_, _, ok := transaction.checkSum()
		if !ok {
			continue
		}
		WriteAheadLog.addCache(transaction)
		WriteAheadLog.fileSize = walReader.bytesRead
	}
}

func (WriteAheadLog *WriteAheadLog) refreshCache() {
	WriteAheadLog.Cache = make(map[uint64][]*Transaction)
}

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

func (writeAheadLog *WriteAheadLog) addCache(transaction Transaction) {
	for _, body := range transaction.Body {
		if writeAheadLog.Cache[body.PageId] == nil {
			writeAheadLog.Cache[body.PageId] = make([]*Transaction, 0)
		}

		writeAheadLog.Cache[body.PageId] = append(writeAheadLog.Cache[body.PageId], &transaction)
	}
}

func (WriteAheadLog *WriteAheadLog) AppendTransaction(transaction Transaction) (error, uint64) {
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
	data = binary.LittleEndian.AppendUint32(data, getChecksumFromBytes(data))

	_, err := WriteAheadLog.Log.Write(data)
	if err != nil {
		return err, WriteAheadLog.nextTransactionId
	}

	WriteAheadLog.nextTransactionId++
	WriteAheadLog.fileSize += uint64(len(data))
	return nil, WriteAheadLog.nextTransactionId - 1
}

func (WriteAheadLog *WriteAheadLog) closeFile() error {
	return WriteAheadLog.Log.Close()
}
