package sstable

import (
	"bytes"
	"testing"
)

type BytesReaderCloser struct {
	*bytes.Reader
}

func (BytesReaderCloser) Close() error {
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

func Test_roundtrip(t *testing.T) {
	input := []Pair{
		Pair{"q", nil},
		Pair{"w", []byte{'a'}},
		Pair{"o", []byte{'b', 'b'}},
		Pair{"p", []byte{'c', 'c', 'c'}},
	}
	var buf bytes.Buffer
	err := Build(&buf, input)
	if err != nil {
		t.Errorf("build err = %#v", err)
		return
	}
	r := BytesReaderCloser{bytes.NewReader(buf.Bytes())}
	tbl, err := New(r)
	if err != nil {
		t.Errorf("load err = %#v", err)
		return
	}
	var output []Pair
	for item := range tbl.All() {
		output = append(output, item)
	}
	i := 0
	j := 0
	for i < len(input) && j < len(output) {
		switch {
		case input[i].Key == output[j].Key && equalBytes(input[i].Value, output[j].Value):
			i++
			j++

		case input[i].Key == output[j].Key:
			t.Errorf("differing values for key %q: expected %v, got %v", input[i].Key, input[i].Value, output[j].Value)
			i++
			j++

		case input[i].Key < output[j].Key:
			t.Errorf("missing key %q", input[i].Key)
			i++

		default:
			t.Errorf("extra key %q", output[j].Key)
			j++
		}
	}
	if len(input) > len(output) {
		t.Errorf("missing %d items", len(input)-len(output))
	}
	if len(input) < len(output) {
		t.Errorf("extra %d items", len(output)-len(input))
	}
}
