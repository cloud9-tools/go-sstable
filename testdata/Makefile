all: file.sstable

clean:
	rm -f json2sstable file.sstable

file.sstable: file.json json2sstable
	./json2sstable --output=$@ < $<

json2sstable:
	go build github.com/cloud9-tools/go-sstable/cmd/json2sstable

.PHONY: all clean
