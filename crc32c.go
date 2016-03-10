package sstable

import (
	"hash/crc32"
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

func crc32c(data []byte) uint32 {
	return crc32.Checksum(data, castagnoliTable)
}

func crc32c_update(crc uint32, data []byte) uint32 {
	return crc32.Update(crc, castagnoliTable, data)
}
