package table

import (
	"encoding/binary"
	"math"
)

type Column struct {
	name     string
	datatype byte
	nullable bool
	length   int32
	offset   int
}

type Schema struct {
	columnCount byte
	bitmapSize  int
	rowSize     int
	columns     []Column
}

func (schema *Schema) SetColumns(columns []Column) {
	schema.columns = columns
	schema.columnCount = byte(len(columns))
	schema.bitmapSize = int(math.Ceil(float64(len(schema.columns) / 8)))
	schema.rowSize = 0
	for _, column := range columns {
		if TYPE_MAP[column.datatype].allowUserLength && column.length != -1 {
			schema.rowSize += int(column.length * TYPE_MAP[column.datatype].defaultSize)
		} else {
			schema.rowSize += int(TYPE_MAP[column.datatype].defaultSize)
		}
	}
}

func (column *Column) GetBinary() []byte {
	response := []byte{}
	response = append(response, column.name...)
	response = append(response, column.datatype)
	if column.nullable {
		response = append(response, 1)
	} else {
		response = append(response, 0)
	}

	if TYPE_MAP[column.datatype].allowUserLength {
		response = binary.LittleEndian.AppendUint32(response, uint32(column.length))
	}

	return response
}

func (schema *Schema) GetBinary() []byte {
	response := []byte{}
	response = append(response, schema.columnCount)
	for _, column := range schema.columns {
		response = append(response, column.GetBinary()...)
	}

	return response
}
