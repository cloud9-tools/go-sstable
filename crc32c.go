package sstable

import (
	"hash/crc32"
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

type crc uint32

func newCRC(data []byte) crc {
	return crc(0).Update(data)
}

func (c crc) Update(data []byte) crc {
	return crc(crc32.Update(uint32(c), castagnoliTable, data))
}

func (c crc) Value() uint32 {
	return uint32((c>>15)|(c<<17)) + 0xa282ead8
}
