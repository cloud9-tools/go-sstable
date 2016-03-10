package sstable

import (
	"errors"
)

var (
	ErrTooManyRecords = errors.New("go-sstable: too many records")
	ErrTooMuchData    = errors.New("go-sstable: too much data (max file size 4 GiB)")
	ErrKeyTooLong     = errors.New("go-sstable: key too long (max key length 255 bytes)")
	ErrValueTooLong   = errors.New("go-sstable: value too long (max value length 4 GiB)")
	ErrWrongLength    = errors.New("go-sstable: wrong length")
	ErrBadFormat      = errors.New("go-sstable: invalid file format")
)

const (
	kmaxuint32   = 0xffffffff
	kmaxkeylen   = 255
)

var magic = [...]byte{'9', 'S', 'S', '0'}
var pad [8]byte
