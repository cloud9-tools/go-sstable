package sstable

import (
	"testing"
)

func TestSSTable_Find(t *testing.T) {
	tbl := dummyTable()
	idx := tbl.Find("q")
	if idx != 2 {
		t.Errorf("Find(q): expected 2, got %d", idx)
	}
	idx = tbl.Find("w")
	if idx != 3 {
		t.Errorf("Find(w): expected 3, got %d", idx)
	}
	idx = tbl.Find("o")
	if idx != 0 {
		t.Errorf("Find(o): expected 0, got %d", idx)
	}
	idx = tbl.Find("p")
	if idx != 1 {
		t.Errorf("Find(p): expected 1, got %d", idx)
	}
}

func TestSSTable_Range(t *testing.T) {
	tbl := dummyTable()
	idx0, idx1 := tbl.Range("p", "s")
	if idx0 != 1 || idx1 != 3 {
		t.Errorf("Range(p,s): expected (1,3), got (%d,%d)", idx0, idx1)
	}

	var keys []string
	for item := range tbl.InRange("p", "s") {
		keys = append(keys, item.Key)
	}
	expectedKeys := []string{"p", "q"}
	if !equalStrings(keys, expectedKeys) {
		t.Errorf("InRange(p,s): expected %v, got %v", expectedKeys, keys)
	}
}

func TestSSTable_Get(t *testing.T) {
	tbl := dummyTable()
	result, err := tbl.Get("o")
	expected := []byte{'b', 'b'}
	switch {
	case err != nil:
		t.Errorf("Get(o): err = %#v", err)

	case !equalBytes(result, expected):
		t.Errorf("Get(o): expected %v, got %v", expected, result)
	}

	result, err = tbl.Get("r")
	switch {
	case err == nil:
		t.Errorf("Get(r): unexpected success: %v", result)

	case err != ErrKeyNotFound:
		t.Errorf("Get(r): unexpected error: %v", err)
	}

	tbl.Close()
}
