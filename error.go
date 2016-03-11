package sstable

import (
	"fmt"

	"github.com/cloud9-tools/go-sstable/internal"
)

type ChecksumError struct {
	Expected uint32
	Actual   uint32
}

func (err ChecksumError) Error() string {
	return fmt.Sprintf("go-sstable: invalid checksum: expected 0x%08x, got 0x%08x", err.Expected, err.Actual)
}

func VerifyChecksum(expected uint32, data []byte) error {
	actual := internal.NewCRC(data).Value()
	if expected != actual {
		return ChecksumError{expected, actual}
	}
	return nil
}
