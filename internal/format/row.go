package format

type Row struct {
	Bitmap  [32]byte
	Mapsize int
	Columns []Item
}

type Item struct {
	DataType byte
	Data     any
}

func (row *Row) getBytes() []byte {
	response := row.Bitmap[:row.Mapsize]
	for _, column := range row.Columns {
		value, _ := TYPE_MAP[column.DataType].getBinary(column.Data)
		response = append(response, value...)
	}
	return response
}

func (row *Row) readBytes(data []byte, schema Schema) {
	bytesRead := 0
	copy(row.Bitmap[:], data[:schema.bitmapSize])
	bytesRead += schema.bitmapSize
	columns := []Item{}
	for _, column := range schema.columns {

		datatype := TYPE_MAP[column.datatype]
		value := datatype.readBinary(data[bytesRead:])
		columns = append(columns, Item{column.datatype, value})
		bytesRead += int(column.length)
	}

	row.Columns = columns

}
