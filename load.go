package sstable

import (
	"encoding/binary"
	"io"
)

func fullReadAt(f io.ReaderAt, data []byte, offset int64) error {
	n, err := f.ReadAt(data, offset)
	if err != nil {
		return err
	}
	if n < len(data) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (t *SSTable) load() error {
	n := len(magic)
	data := make([]byte, n+8)
	if err := fullReadAt(t.f, data, 0); err != nil {
		return err
	}
	pos := int64(len(data))

	if string(data[0:n]) != magic {
		return ErrBadFormat
	}
	count := binary.BigEndian.Uint32(data[n : n+4])
	n += 4
	cksum := binary.BigEndian.Uint32(data[n : n+4])
	if err := VerifyChecksum(cksum, data[0:n]); err != nil {
		return err
	}

	r := make([]record, count)
	for i := uint32(0); i < count; i++ {
		data = make([]byte, 16)
		if err := fullReadAt(t.f, data, pos); err != nil {
			return err
		}
		pos += int64(len(data))

		key := make([]byte, data[0])
		r[i].length = (uint32(data[1]) << 16) | (uint32(data[2]) << 8) | uint32(data[3])
		r[i].offset = binary.BigEndian.Uint32(data[4:8])
		r[i].cksum = binary.BigEndian.Uint32(data[8:12])
		stored := binary.BigEndian.Uint32(data[12:16])
		if err := fullReadAt(t.f, key, pos); err != nil {
			return err
		}
		pos += int64(len(key))
		r[i].key = string(key)
		data = data[0:12]
		data = append(data, key...)
		if err := VerifyChecksum(stored, data); err != nil {
			return err
		}
	}
	t.r = r
	return nil
}
