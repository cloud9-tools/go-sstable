package sstable

import (
	"bytes"
	"testing"
)

func TestBuild_simple(t *testing.T) {
	var buf bytes.Buffer
	err := Build(&buf, []Pair{
		Pair{"q", nil},
		Pair{"w", []byte{'a'}},
		Pair{"o", []byte{'b', 'b'}},
		Pair{"p", []byte{'c', 'c', 'c'}},
	})
	if err != nil {
		t.Errorf("err = %#v", err)
		return
	}
	actual := buf.Bytes()
	expected := []byte{
		'9', 'S', 'S', '0', // magic
		0, 0, 0, 4, // 4 pairs
		0, 0, 0, 2, // first pair: length 2
		0, 0, 0, 0x34, // first pair: offset 52
		1, 'o', // first pair: name "o"
		0, 0, 0, 3, // second pair: length 3
		0, 0, 0, 0x3a, // second pair: offset 58
		1, 'p', // second pair: name "p"
		0, 0, 0, 0, // third pair: length 0
		0, 0, 0, 0x41, // third pair: offset 65
		1, 'q', // third pair: name "q"
		0, 0, 0, 1, // fourth pair: length 1
		0, 0, 0, 0x45, // fourth pair: offset 69
		1, 'w', // fourth pair: name "w"
		0xD5, 0x34, 0x8D, 0xB8, // index crc32c
		'b', 'b', // 2 data
		0xD6, 0x45, 0x81, 0xAF, // value crc32c
		'c', 'c', 'c', // 3 data
		0x6A, 0x86, 0xF5, 0xCD, // value crc32c
		0x00, 0x00, 0x00, 0x00, // value crc32c
		'a',                    // 1 data
		0xC1, 0xD0, 0x43, 0x30, // value crc32c
	}
	for i := 0; i < len(expected); i++ {
		if i >= len(actual) {
			t.Errorf("shorter than expected: want %d, got %d", len(expected), len(actual))
			break
		}
		if actual[i] != expected[i] {
			j := i + 1
			for j < len(actual) && j < len(expected) {
				if actual[j] == expected[j] {
					break
				}
				j++
			}
			if j > i+8 {
				k := i + 8
				t.Errorf("data mismatch: at [%d:%d] want %v..., got %v...", i, j, expected[i:k], actual[i:k])
			} else {
				t.Errorf("data mismatch: at [%d:%d] want %v, got %v", i, j, expected[i:j], actual[i:j])
			}
			i = j - 1
		}
	}
	if len(actual) > len(expected) {
		t.Errorf("longer than expected: want %d, got %d", len(expected), len(actual))
	}
}
