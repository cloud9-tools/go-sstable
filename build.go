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
	for _, item := range data {
		maxoffset += uint64(len(item.Value))
	}
	if maxoffset > kmaxuint32 {
		return ErrTooMuchData
	}

	sort.Sort(byKey(data))

	var tmp [256]byte

	_, err := w.Write([]byte(magic))
	if err != nil {
		return err
	}

	binary.BigEndian.PutUint32(tmp[0:4], uint32(len(data)))
	_, err = w.Write(tmp[0:4])
	if err != nil {
		return err
	}

	var offset = 4 + uint32(len(magic))
	for _, item := range data {
		offset += 9 + uint32(len(item.Key))
	}

	for _, item := range data {
		binary.BigEndian.PutUint32(tmp[0:4], uint32(len(item.Value)))
		binary.BigEndian.PutUint32(tmp[4:8], offset)
		_, err = w.Write(tmp[0:8])
		if err != nil {
			return err
		}

		tmp[0] = uint8(len(item.Key))
		copy(tmp[1:], []byte(item.Key))
		_, err = w.Write(tmp[0 : len(item.Key)+1])
		if err != nil {
			return err
		}

		offset += uint32(len(item.Value))
	}

	for _, item := range data {
		_, err = w.Write(item.Value)
		if err != nil {
			return err
		}
	}

	return nil
}
