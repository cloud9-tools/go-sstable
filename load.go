package sstable

import (
	"encoding/binary"
	"io"
	"os"
	"sort"
)

type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

type SSTable struct {
	f ReadSeekCloser
	r []record
}

type record struct {
	key    string
	length uint32
	offset uint32
}

func New(f ReadSeekCloser) (*SSTable, error) {
	t := &SSTable{f, nil}
	err := t.load()
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (t *SSTable) Close() error {
	return t.f.Close()
}

func (t *SSTable) Len() int {
	return len(t.r)
}

func (t *SSTable) Key(idx int) string {
	return t.r[idx].key
}

func (t *SSTable) Value(idx int) ([]byte, error) {
	_, err := t.f.Seek(int64(t.r[idx].offset), 0)
	if err != nil {
		return nil, err
	}
	data := make([]byte, t.r[idx].length)
	n, err := t.f.Read(data)
	if err != nil {
		return nil, err
	}
	if n < len(data) {
		return nil, io.ErrUnexpectedEOF
	}
	return data, nil
}

func (t *SSTable) At(idx int) (Pair, error) {
	k := t.Key(idx)
	v, err := t.Value(idx)
	if err != nil {
		return Pair{}, err
	}
	return Pair{k, v}, nil
}

func (t *SSTable) In(i, j int) <-chan Pair {
	ch := make(chan Pair)
	go (func() {
		for idx := i; idx < j; idx++ {
			item, err := t.At(idx)
			if err != nil {
				panic(err)
			}
			ch <- item
		}
		close(ch)
	})()
	return ch
}

func (t *SSTable) All() <-chan Pair {
	return t.In(0, t.Len())
}

func (t *SSTable) Find(key string) int {
	i := sort.Search(len(t.r), func(idx int) bool {
		return t.r[idx].key >= key
	})
	if i < len(t.r) && t.r[i].key == key {
		return i
	}
	return -1
}

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
