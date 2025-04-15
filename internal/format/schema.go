package format

import (
	"encoding/binary"
	"math"
)

type Column struct {
	name     string
	datatype byte
	nullable bool
	length   int32 // length of column in bytes
	offset   int   // offset in bytes from start of rowdata including null bitmap
}

type Schema struct {
	columnCount byte
	bitmapSize  int
	rowSize     int
	columns     []Column
}

func (column *Column) SetDataType(dataType byte, length int32) {
	column.datatype = dataType
	if TYPE_MAP[dataType].allowUserLength {
		column.length = TYPE_MAP[dataType].defaultSize * length
	} else {
		column.length = TYPE_MAP[dataType].defaultSize
	}
}

func (column *Column) GetBinary() []byte {
	response := []byte{}
	response = append(response, byte(len(column.name)))
	response = append(response, column.name...)
	response = append(response, column.datatype)
	if column.nullable {
		response = append(response, 1)
	} else {
		response = append(response, 0)
	}

	if TYPE_MAP[column.datatype].allowUserLength {
		response = binary.LittleEndian.AppendUint16(response, uint16(column.length))
	}

	return response
}

func (column *Column) ReadBinary(data []byte) int {
	bytesRead := 0
	nameLen := data[0]
	bytesRead++

	column.name = string(data[bytesRead : bytesRead+int(nameLen)])
	bytesRead += int(nameLen)

	column.datatype = data[nameLen]
	bytesRead++

	column.nullable = data[nameLen+1] == 1
	bytesRead++

	if TYPE_MAP[column.datatype].allowUserLength {
		column.length = int32(binary.LittleEndian.Uint16(data[bytesRead:]))
		bytesRead += 2
	} else {
		column.length = TYPE_MAP[column.datatype].defaultSize
	}

	return bytesRead
}

func (schema *Schema) SetColumns(columns []Column) {
	schema.columns = columns
	schema.columnCount = byte(len(columns))
	schema.bitmapSize = int(math.Ceil(float64(len(schema.columns) / 8)))
	schema.rowSize = schema.bitmapSize
	for i, column := range schema.columns {
		schema.columns[i].offset = schema.rowSize
		schema.rowSize += int(column.length)
	}
}

func (schema *Schema) GetBinary() []byte {
	response := []byte{}
	response = append(response, schema.columnCount)
	for _, column := range schema.columns {
		response = append(response, column.GetBinary()...)
	}

	return response
}

func (schema *Schema) ReadBinary(data []byte) {
	bytesRead := 0
	columnCount := data[0]
	bytesRead++

	columns := []Column{}
	for i := 0; i < int(columnCount); i++ {
		column := Column{}
		bytesRead += column.ReadBinary(data[bytesRead:])
		columns = append(columns, column)
	}

	schema.SetColumns(columns)
}
