package sstable

import (
	"io"
	"sort"
)

type ReadAtCloser interface {
	io.ReaderAt
	io.Closer
}

type SSTable struct {
	f ReadAtCloser
	r []record
	m []byte
}

type record struct {
	key    string
	length uint32
	offset uint32
}

func New(f ReadAtCloser) (*SSTable, error) {
	t := &SSTable{f, nil, nil}
	if err := t.load(); err != nil {
		return nil, err
	}
	if err := t.tryMMap(); err != nil && err != errNotImplemented {
		return nil, err
	}
	return t, nil
}

func (t *SSTable) Close() error {
	if t.m != nil {
		t.tryMunmap()
		t.m = nil
	}
	return t.f.Close()
}

func (t *SSTable) Len() int {
	return len(t.r)
}

func (t *SSTable) Key(idx int) string {
	return t.r[idx].key
}

func (t *SSTable) Value(idx int) ([]byte, error) {
	if t.m != nil {
		p := t.r[idx].offset
		q := t.r[idx].length + p
		return t.m[p:q], nil
	}
	data := make([]byte, t.r[idx].length)
	n, err := t.f.ReadAt(data, int64(t.r[idx].offset))
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

func (t *SSTable) Find(key string) int {
	i := sort.Search(len(t.r), func(idx int) bool {
		return t.r[idx].key >= key
	})
	if i < len(t.r) && t.r[i].key == key {
		return i
	}
	return -1
}

func (t *SSTable) Get(key string) ([]byte, error) {
	i := t.Find(key)
	if i >= 0 {
		return t.Value(i)
	}
	return nil, ErrKeyNotFound
}

func (t *SSTable) Range(lo, hi string) (int, int) {
	i := sort.Search(len(t.r), func(idx int) bool {
		return t.r[idx].key >= lo
	})
	j := sort.Search(len(t.r), func(idx int) bool {
		return t.r[idx].key >= hi
	})
	return i, j
}

func (t *SSTable) KeysIn(i, j int) <-chan string {
	ch := make(chan string)
	go (func() {
		for idx := i; idx < j; idx++ {
			ch <- t.Key(idx)
		}
		close(ch)
	})()
	return ch
}

func (t *SSTable) KeysInRange(lo, hi string) <-chan string {
	i, j := t.Range(lo, hi)
	return t.KeysIn(i, j)
}

func (t *SSTable) AllKeys() <-chan string {
	return t.KeysIn(0, t.Len())
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

func (t *SSTable) InRange(lo, hi string) <-chan Pair {
	i, j := t.Range(lo, hi)
	return t.In(i, j)
}

func (t *SSTable) All() <-chan Pair {
	return t.In(0, t.Len())
}
