package sstable

import (
	"hash/crc32"
)

const kmaskdelta = 0xa282ead8

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

func crc32c(data []byte) uint32 {
	return crc32.Checksum(data, castagnoliTable)
}

func crc32c_update(crc uint32, data []byte) uint32 {
	return crc32.Update(crc, castagnoliTable, data)
}

func crc_mask(crc uint32) uint32 {
	return ((crc >> 15) | (crc << 17)) + kmaskdelta
}

func crc_unmask(crc uint32) uint32 {
	tmp := crc - kmaskdelta
	return ((tmp >> 17) | (tmp << 15))
}
