package sstable

import (
	"os"
	"syscall"
)

func (t *SSTable) tryMMap() error {
	f, ok := t.f.(*os.File)
	if !ok {
		return errNotImplemented
	}

	fi, err := f.Stat()
	if err != nil {
		return err
	}

	if fi.Size() > int64(kmaxint) {
		return errNotImplemented
	}

	mmap, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return errNotImplemented
	}

	t.m = mmap
	return nil
}

func (t *SSTable) tryMunmap() {
	syscall.Munmap(t.m)
}
