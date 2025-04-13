package table

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
	},
}

type TypeInfo struct {
	name            string
	fixed           bool // does the type support variable size like varchar
	allowUserLength bool // does it allow user defined sizes like char(6)
	defaultSize     int  // in bytes
}
