package sstable

import (
	"sort"
)

type Pair struct {
	Key   string
	Value []byte
}

type byKey []Pair

func (list byKey) Len() int {
	return len(list)
}
func (list byKey) Less(i, j int) bool {
	return list[i].Key < list[j].Key
}
func (list byKey) Swap(i, j int) {
	list[i], list[j] = list[j], list[i]
}

var _ sort.Interface = byKey([]Pair(nil))
