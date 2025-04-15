package format

import "encoding/binary"

const (
	TYPE_INT = iota
)

// keep sequence same as the constants above
var TYPE_MAP = []TypeInfo{
	{
		"int",
		true,
		false,
		4,
		func(data any) ([]byte, bool) {
			value, ok := data.(int32)
			if !ok {
				return []byte{}, false
			}
			return binary.LittleEndian.AppendUint32([]byte{}, uint32(value)), true
		},
		func(data []byte) any {
			return int32(binary.LittleEndian.Uint32(data))
		},
	},
}

type TypeInfo struct {
	name            string
	fixed           bool  // does the type support variable size like varchar
	allowUserLength bool  // does it allow user defined sizes like char(6)
	defaultSize     int32 // in bytes
	getBinary       func(any) ([]byte, bool)
	readBinary      func([]byte) any
}
