package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/cloud9-tools/go-sstable"
)

var output = flag.String("output", "", "SSTable file to generate")

func main() {
	flag.Parse()
	if *output == "" {
		flag.Usage()
		fmt.Fprintln(os.Stderr, "error: missing required flag -output=")
		os.Exit(1)
	}
	if flag.NArg() != 0 {
		flag.Usage()
		fmt.Fprintf(os.Stderr, "error: %d unexpected positional argument(s)\n", flag.NArg())
		os.Exit(1)
	}

	inbytes, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		panic(err)
	}

	inmap := make(map[string][]byte)
	err = json.Unmarshal(inbytes, &inmap)
	if err != nil {
		panic(err)
	}

	outfile, err := os.OpenFile(*output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		panic(err)
	}
	defer outfile.Close()

	var input []sstable.Pair
	for k, v := range inmap {
		input = append(input, sstable.Pair{k, v})
	}
	err = sstable.Build(outfile, input)
	if err != nil {
		panic(err)
	}
}
