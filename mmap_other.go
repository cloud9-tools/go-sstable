// +build !linux

package sstable

func (t *SSTable) tryMMap() error {
	return errNotImplemented
}

func (t *SSTable) tryMunmap() {
}
