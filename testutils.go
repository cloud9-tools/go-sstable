package sstable

import (
	"bytes"
)

type bytesReaderCloser struct {
	*bytes.Reader
}

func (bytesReaderCloser) Close() error {
	return nil
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func dummyTable() *SSTable {
	data := []byte{'a', 'b', 'b', 'c', 'c', 'c'}
	return &SSTable{
		f: bytesReaderCloser{bytes.NewReader(data)},
		r: []record{
			record{"o", 2, 1, 0xa5e29763},
			record{"p", 3, 3, 0x8e1dbfe5},
			record{"q", 0, 0, 0xa282ead8},
			record{"w", 1, 0, 0x28e46e78},
		},
	}
}
