package sstable

import (
	"errors"
)

var (
	ErrTooManyRecords = errors.New("go-sstable: too many records")
	ErrTooMuchData    = errors.New("go-sstable: too much data (max file size 4 GiB)")
	ErrKeyTooLong     = errors.New("go-sstable: key too long (max key length 255 bytes)")
	ErrValueTooLong   = errors.New("go-sstable: value too long (max value length 4 GiB)")
	ErrBadFormat      = errors.New("go-sstable: invalid file format")
	ErrKeyNotFound    = errors.New("go-sstable: key not found")
)

const (
	kmaxint    = int(^uint(0) >> 1)
	kmaxuint32 = 0xffffffff
	kmaxkeylen = 255
	magic      = "9SS0"
)

var errNotImplemented = errors.New("not implemented")
