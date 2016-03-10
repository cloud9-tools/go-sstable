package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/cloud9-tools/go-sstable"
)

var input = flag.String("input", "", "SSTable file to read")

func main() {
	flag.Parse()
	if *input == "" {
		flag.Usage()
		fmt.Fprintln(os.Stderr, "error: missing required flag -input=")
		os.Exit(1)
	}
	if flag.NArg() != 0 {
		flag.Usage()
		fmt.Fprintf(os.Stderr, "error: %d unexpected positional argument(s)\n", flag.NArg())
		os.Exit(1)
	}

	f, err := os.Open(*input)
	if err != nil {
		panic(err)
	}

	t, err := sstable.New(f)
	if err != nil {
		f.Close()
		panic(err)
	}
	defer t.Close()

	output := make(map[string][]byte, t.Len())
	for item := range t.All() {
		output[item.Key] = item.Value
	}

	data, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}

	_, err = os.Stdout.Write(data)
	if err != nil {
		panic(err)
	}
	_, err = os.Stdout.Write([]byte{'\n'})
	if err != nil {
		panic(err)
	}
}
