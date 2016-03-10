package sstable

import (
	"encoding/binary"
	"io"
	"sort"
)

func Build(w io.Writer, data []Pair) error {
	if len(data) > kmaxuint32 {
		return ErrTooManyRecords
	}
	var maxoffset = 4 + uint64(len(magic))
	for _, item := range data {
		if len(item.Key) > kmaxkeylen {
			return ErrKeyTooLong
		}
		if len(item.Value) > kmaxuint32 {
			return ErrValueTooLong
		}
		maxoffset += 9 + uint64(len(item.Key))
	}
	maxoffset += 4
	for _, item := range data {
		maxoffset += 4 + uint64(len(item.Value))
	}
	if maxoffset > kmaxuint32 {
		return ErrTooMuchData
	}

	sort.Sort(byKey(data))

	var tmp [256]byte
	n := len(magic)
	copy(tmp[0:n], []byte(magic))
	binary.BigEndian.PutUint32(tmp[n:n+4], uint32(len(data)))
	n += 4
	cksum := crc32c(tmp[0:n])
	if _, err := w.Write(tmp[0:n]); err != nil {
		return err
	}

	var offset = 4 + uint32(len(magic))
	for _, item := range data {
		offset += 9 + uint32(len(item.Key))
	}
	offset += 4

	for _, item := range data {
		binary.BigEndian.PutUint32(tmp[0:4], uint32(len(item.Value)))
		binary.BigEndian.PutUint32(tmp[4:8], offset)
		tmp[8] = uint8(len(item.Key))
		copy(tmp[9:], []byte(item.Key))
		n := 9 + len(item.Key)

		cksum = crc32c_update(cksum, tmp[0:n])
		if _, err := w.Write(tmp[0:n]); err != nil {
			return err
		}

		offset += 4 + uint32(len(item.Value))
	}

	binary.BigEndian.PutUint32(tmp[0:4], cksum)
	if _, err := w.Write(tmp[0:4]); err != nil {
		return err
	}

	for _, item := range data {
		if _, err := w.Write(item.Value); err != nil {
			return err
		}
		binary.BigEndian.PutUint32(tmp[0:4], crc32c(item.Value))
		if _, err := w.Write(tmp[0:4]); err != nil {
			return err
		}
	}

	return nil
}
