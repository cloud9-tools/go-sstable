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
	data := make([]byte, 4+len(magic))
	if err := fullReadAt(t.f, data, 0); err != nil {
		return err
	}
	pos := int64(len(data))

	if string(data[0:len(magic)]) != magic {
		return ErrBadFormat
	}
	count := binary.BigEndian.Uint32(data[len(magic):])
	r := make([]record, count)
	for i := uint32(0); i < count; i++ {
		data = make([]byte, 9)
		if err := fullReadAt(t.f, data, pos); err != nil {
			return err
		}
		pos += int64(len(data))
		r[i].length = binary.BigEndian.Uint32(data[0:4])
		r[i].offset = binary.BigEndian.Uint32(data[4:8])
		key := make([]byte, data[8])
		if err := fullReadAt(t.f, key, pos); err != nil {
			return err
		}
		pos += int64(len(key))
		r[i].key = string(key)
	}
	t.r = r
	return nil
}
