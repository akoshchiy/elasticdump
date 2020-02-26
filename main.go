package main

import (
	"flag"
	"github.com/cheggaaa/pb/v3"
	es "github.com/elastic/go-elasticsearch/v5"
	"log"
)

type Args struct {
	Url       string
	Index     string
	Type      string
	BatchSize int
	Compress  bool
}

func parseArgs() Args {
	url := flag.String("url", "http://localhost:9200", "elasticsearch url")
	compress := flag.Bool("compress", false, "compress dump")
	index := flag.String("index", "", "index to dump")
	t := flag.String("type", "", "type of dump document")
	batchSize := flag.Int("batchSize", 1000, "max doc count per request")

	flag.Parse()

	return Args{
		Url:       *url,
		Compress:  *compress,
		Index:     *index,
		Type:      *t,
		BatchSize: *batchSize,
	}
}

func main() {
	args := parseArgs()
	client, err := es.NewClient(es.Config{
		Addresses: []string{
			args.Url,
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	dumper := NewDumper(client, NewPool(10, 10))

	result, err := dumper.Dump(args)
	if err != nil {
		log.Fatal(err)
	}

	writer, err := NewWriter(args.Index+".json", 10)
	if err != nil {
		log.Fatal(err)
	}

	println(args.Index)
	bar := pb.StartNew(result.Size)

	for doc := range result.Docs {
		//println(doc)
		writer.Append(doc)
		bar.Increment()
	}
	writer.Close()
	println("\nDONE!")
}
