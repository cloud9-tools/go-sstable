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
		// header
		'9', 'S', 'S', '0', // magic
		0, 0, 0, 4, // 4 pairs
		0x3B, 0x18, 0xDD, 0x01, // header checksum
		// first pair
		1,       // key length
		0, 0, 2, // value length
		0, 0, 0, 80, // value offset
		0xA5, 0xE2, 0x97, 0x63, // value checksum
		0xF9, 0x2A, 0xBC, 0xD8, // entry checksum
		'o', // name
		// second pair
		1,       // key length
		0, 0, 3, // value length
		0, 0, 0, 82, // value offset
		0x8E, 0x1D, 0xBF, 0xE5, // value checksum
		0xA8, 0xAB, 0x6F, 0xBB, // entry checksum
		'p', // name
		// third pair
		1,       // key length
		0, 0, 0, // value length
		0, 0, 0, 85, // value offset
		0xA2, 0x82, 0xEA, 0xD8, // value checksum
		0x2F, 0x38, 0xFB, 0xA2, // entry checksum
		'q', // name
		// fourth pair
		1,       // key length
		0, 0, 1, // value length
		0, 0, 0, 85, // value offset
		0x28, 0xE4, 0x6E, 0x78, // value checksum
		0x33, 0x01, 0x18, 0xDC, // entry checksum
		'w', // name
		// data
		'b', 'b', 'c', 'c', 'c', 'a',
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
