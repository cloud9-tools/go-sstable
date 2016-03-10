package sstable

import (
	"os"
	"testing"
)

func TestSSTable_osfile(t *testing.T) {
	f, err := os.Open("testdata/file.sstable")
	if err != nil {
		panic(err)
	}

	tbl, err := New(f)
	if err != nil {
		f.Close()
		t.Errorf("err = %#v", err)
		return
	}
	defer tbl.Close()

	var keys []string
	for key := range tbl.KeysInRange("a", "b") {
		keys = append(keys, key)
	}
	if expected := []string{"a", "aaa"}; !equalStrings(keys, expected) {
		t.Errorf("expected %v, got %v", expected, keys)
	}

	data, err := tbl.Get("z")
	if err != nil {
		t.Errorf("err = %#v", err)
		return
	}
	if expected := "Hello, I am \"z\".\n"; string(data) != expected {
		t.Errorf("expected %v, got %v", []byte(expected), data)
	}
}
