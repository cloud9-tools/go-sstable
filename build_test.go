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
		0, 0, 0, 0x30, // first pair: offset 48
		1, 'o', // first pair: name "o"
		0, 0, 0, 3, // second pair: length 3
		0, 0, 0, 0x32, // second pair: offset 50
		1, 'p', // second pair: name "p"
		0, 0, 0, 0, // third pair: length 0
		0, 0, 0, 0x35, // third pair: offset 53
		1, 'q', // third pair: name "q"
		0, 0, 0, 1, // fourth pair: length 1
		0, 0, 0, 0x35, // fourth pair: offset 53
		1, 'w', // fourth pair: name "w"
		// <-- offset 48
		'b', 'b', // 2 data
		// <-- offset 56
		'c', 'c', 'c', // 3 data
		// <-- offset 64
		'a', // 1 data
	}
	for i := 0; i < len(expected); i++ {
		if i >= len(actual) {
			t.Errorf("actual shorter than expected: %d vs %d", len(actual), len(expected))
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
				t.Errorf("actual differs from expected: [%d:%d] %v... %v...", i, j, actual[i:k], expected[i:k])
			} else {
				t.Errorf("actual differs from expected: [%d:%d] %v %v", i, j, actual[i:j], expected[i:j])
			}
			i = j - 1
		}
	}
	if len(actual) > len(expected) {
		t.Errorf("actual longer than expected: %d vs %d", len(actual), len(expected))
	}
}
