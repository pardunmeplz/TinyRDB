package storage

import "hash/crc32"

// PageData represents the data portion of a page, excluding the header.
// It's a fixed-size array of bytes with a size of DefaultPageSize - PageHeaderSize.
type PageData *[DefaultPageSize - PageHeaderSize]byte

// Page represents a complete database page, containing both header and data.
type Page struct {
	Header PageHeader // Page metadata and integrity information
	Data   PageData   // Actual page data
}

// PageHeader contains metadata about a page, including:
// - Version for future schema changes
// - Type to identify page purpose
// - Checksum for data integrity verification
type PageHeader struct {
	PageVersion byte   // Version number for page format
	PageType    byte   // Type of page (metadata, user data, etc.)
	Checksum    uint32 // CRC32 checksum of page data
}

// getChecksum calculates a CRC32 checksum for the page data
func getChecksum(data PageData) uint32 {
	return crc32.ChecksumIEEE(data[:])
}

// MakePageData creates a new empty page data buffer
func MakePageData() PageData {
	value := [DefaultPageSize - PageHeaderSize]byte{}
	return &value
}

// Page header layout constants
const (
	PageHeaderSize           = 6 // Total size of page header in bytes
	PageHeaderVersionOffset  = 0 // Offset to page version byte
	PageHeaderTypeOffset     = 1 // Offset to page type byte
	PageHeaderChecksumOffset = 2 // Offset to checksum (4 bytes)
)

// Metadata page layout constants
const (
	MetadataFreeListHeadOffset = 0 + PageHeaderSize  // Offset to free list head pointer
	MetadataTotalPageOffset    = 8 + PageHeaderSize  // Offset to total page count
	MetadataPageSizeOffset     = 16 + PageHeaderSize // Offset to page size
)

// Page type constants
// These define the different types of pages in the database
const (
	PagetypeMetadata  = iota // Page containing database metadata
	PagetypeUserdata         // Page containing user data
	PagetypeFreepage         // Page in the free list
	PagetypeSchema           // Page containing schema information
	PagetypeTableData        // Page containing table data
	PageTypeOverflow         // Page for overflow data
	PageTypeIndex            // Page containing index data
)

// DefaultPageSize is the standard size of a database page (4KB)
const DefaultPageSize = 4096
