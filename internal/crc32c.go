package internal

import (
	"hash/crc32"
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

type CRC uint32

func NewCRC(data []byte) CRC {
	return CRC(0).Update(data)
}

func (c CRC) Update(data []byte) CRC {
	return CRC(crc32.Update(uint32(c), castagnoliTable, data))
}

func (c CRC) Value() uint32 {
	return uint32((c>>15)|(c<<17)) + 0xa282ead8
}
