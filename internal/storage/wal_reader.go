package storage

import (
	"bufio"
	"encoding/binary"
	"io"
)

type WalReader struct {
	WriteAheadLog *WriteAheadLog
	reader        io.Reader
}

func (WriteAheadLog *WriteAheadLog) Startup() error {
	WalReader := WalReader{}
	WalReader.initialize(WriteAheadLog)
	_, err := WalReader.getTransaction()
	if err != nil {
		return err
	}
	return nil
}

func (WalReader *WalReader) initialize(WriteAheadLog *WriteAheadLog) {
	WalReader.reader = bufio.NewReader(WriteAheadLog.Log)
	WalReader.WriteAheadLog = WriteAheadLog
	WriteAheadLog.Log.Seek(0, io.SeekStart)
}

func (WalReader *WalReader) getTransaction() (Transaction, error) {
	transaction := Transaction{}
	transaction.MakeTransaction()

	err := binary.Read(WalReader.reader, binary.LittleEndian, &transaction.Header.transactionId)
	if err != nil {
		return transaction, err
	}
	err = binary.Read(WalReader.reader, binary.LittleEndian, &transaction.Header.pageCount)
	if err != nil {
		return transaction, err
	}

	for range transaction.Header.pageCount {
		body := PageEntry{}

		err = binary.Read(WalReader.reader, binary.LittleEndian, &body.PageId)
		if err != nil {
			return transaction, err
		}

		err = binary.Read(WalReader.reader, binary.LittleEndian, &body.Offset)
		if err != nil {
			return transaction, err
		}

		err = binary.Read(WalReader.reader, binary.LittleEndian, &body.Length)
		if err != nil {
			return transaction, err
		}

		body.OldData = make([]byte, body.Length)
		err = binary.Read(WalReader.reader, binary.LittleEndian, body.OldData)
		if err != nil {
			return transaction, err
		}

		body.NewData = make([]byte, body.Length)
		err = binary.Read(WalReader.reader, binary.LittleEndian, body.NewData)
		if err != nil {
			return transaction, err
		}
		transaction.Body = append(transaction.Body, body)
	}

	err = binary.Read(WalReader.reader, binary.LittleEndian, &transaction.End.TransactionId)
	if err != nil {
		return transaction, err
	}

	err = binary.Read(WalReader.reader, binary.LittleEndian, &transaction.End.Status)
	if err != nil {
		return transaction, err
	}

	err = binary.Read(WalReader.reader, binary.LittleEndian, &transaction.End.Checksum)
	return transaction, nil
}
