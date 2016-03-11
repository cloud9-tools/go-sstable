package sstable

import (
	"encoding/binary"
	"io"
	"sort"
)

func Build(w io.Writer, data []Pair) error {
	sort.Sort(byKey(data))

	if len(data) > kmaxuint32 {
		return ErrTooManyRecords
	}
	var offset = 8 + uint64(len(magic))
	for _, item := range data {
		if len(item.Key) > kmaxkeylen {
			return ErrKeyTooLong
		}
		if len(item.Value) > kmaxvaluelen {
			return ErrValueTooLong
		}
		offset += 16 + uint64(len(item.Key))
	}
	maxoffset := offset
	for _, item := range data {
		maxoffset += uint64(len(item.Value))
	}
	if maxoffset > kmaxuint32 {
		return ErrTooMuchData
	}

	n := len(magic)
	var tmp [16]byte
	copy(tmp[0:n], []byte(magic))
	binary.BigEndian.PutUint32(tmp[n:n+4], uint32(len(data)))
	n += 4
	binary.BigEndian.PutUint32(tmp[n:n+4], crc_mask(crc32c(tmp[0:n])))
	n += 4
	if _, err := w.Write(tmp[0:n]); err != nil {
		return err
	}

	for _, item := range data {
		lenKey := uint32(len(item.Key))
		lenValue := uint32(len(item.Value))
		tmp[0] = uint8(lenKey)
		tmp[1] = uint8(lenValue >> 16)
		tmp[2] = uint8(lenValue >> 8)
		tmp[3] = uint8(lenValue)
		binary.BigEndian.PutUint32(tmp[4:8], uint32(offset))
		binary.BigEndian.PutUint32(tmp[8:12], crc_mask(crc32c(item.Value)))
		cksum := crc32c(tmp[0:12])
		cksum = crc32c_update(cksum, []byte(item.Key))
		binary.BigEndian.PutUint32(tmp[12:16], crc_mask(cksum))
		if _, err := w.Write(tmp[0:16]); err != nil {
			return err
		}
		if _, err := w.Write([]byte(item.Key)); err != nil {
			return err
		}
		offset += uint64(len(item.Value))
	}

	for _, item := range data {
		if _, err := w.Write(item.Value); err != nil {
			return err
		}
	}

	return nil
}
