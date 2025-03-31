package storage

import "hash/crc32"

type PageData *[DefaultPageSize - PageHeaderSize]byte

type Page struct {
	Header PageHeader
	Data   PageData
}

type PageHeader struct {
	PageVersion byte
	PageType    byte
	Checksum    uint32
}

func getChecksum(data PageData) uint32 {
	return crc32.ChecksumIEEE(data[:])
}

func MakePageData() PageData {
	value := [DefaultPageSize - PageHeaderSize]byte{}
	return &value
}

// page header offsets
const (
	PageHeaderSize           = 6
	PageHeaderVersionOffset  = 0
	PageHeaderTypeOffset     = 1
	PageHeaderChecksumOffset = 2
)

// metadata offsets
const (
	MetadataFreeListHeadOffset = 0 + PageHeaderSize
	MetadataTotalPageOffset    = 8 + PageHeaderSize
	MetadataPageSizeOffset     = 16 + PageHeaderSize
)

// 1 byte header so max 255 types
const (
	PagetypeMetadata = iota
	PagetypeUserdata
	PagetypeFreepage
)

const DefaultPageSize = 4096
