package sstable

import (
	"fmt"
)

func ExampleVerifyChecksum_pass() {
	err := VerifyChecksum(0x21f1576e, []byte("abc"))
	fmt.Printf("%#v\n", err)
	fmt.Println(err)
	// Output:
	// <nil>
	// <nil>
}

func ExampleVerifyChecksum_fail() {
	err := VerifyChecksum(0, []byte("abc"))
	fmt.Printf("%#v\n", err)
	fmt.Println(err)
	// Output:
	// sstable.ChecksumError{Expected:0x0, Actual:0x21f1576e}
	// go-sstable: invalid checksum: expected 0x00000000, got 0x21f1576e
}
