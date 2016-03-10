package sstable

import (
	"encoding/binary"
	"io"
)

func (t *SSTable) load() error {
	data := make([]byte, 4+len(magic))
	n, err := t.f.Read(data)
	if err != nil {
		return err
	}
	if n < len(data) {
		return io.ErrUnexpectedEOF
	}
	if string(data[0:len(magic)]) != magic {
		return ErrBadFormat
	}
	count := binary.BigEndian.Uint32(data[len(magic):])
	r := make([]record, count)
	for i := uint32(0); i < count; i++ {
		data = make([]byte, 9)
		n, err = t.f.Read(data)
		if err != nil {
			return err
		}
		if n < len(data) {
			return io.ErrUnexpectedEOF
		}
		r[i].length = binary.BigEndian.Uint32(data[0:4])
		r[i].offset = binary.BigEndian.Uint32(data[4:8])
		key := make([]byte, data[8])
		n, err = t.f.Read(key)
		if err != nil {
			return err
		}
		if n < len(key) {
			return io.ErrUnexpectedEOF
		}
		r[i].key = string(key)
	}
	t.r = r
	return nil
}
