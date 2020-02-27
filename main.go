package main

import (
	"compress/gzip"
	"flag"
	"github.com/cheggaaa/pb/v3"
	es "github.com/elastic/go-elasticsearch/v5"
	"io"
	"log"
	"os"
)

type Args struct {
	Url                   string
	Index                 string
	Type                  string
	BatchSize             int
	Compress              bool
	MaxConcurrentRequests int
}

func parseArgs() Args {
	url := flag.String("url", "http://localhost:9200", "elasticsearch url")
	compress := flag.Bool("compress", false, "write dump with gzip compression (default false)")
	index := flag.String("index", "", "index to dump")
	t := flag.String("type", "", "type of dump document")
	batchSize := flag.Int("batchSize", 1000, "max doc count per request")
	maxConcurrentRequests := flag.Int("maxConcurrentRequests", 10, "max concurrent requests to elasticsearch")

	flag.Parse()

	if *index == "" || *t == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	return Args{
		Url:                   *url,
		Compress:              *compress,
		Index:                 *index,
		Type:                  *t,
		BatchSize:             *batchSize,
		MaxConcurrentRequests: *maxConcurrentRequests,
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

	fw, err := buildFileWriter(args)
	if err != nil {
		log.Fatal(err)
	}
	writer, err := NewDumpWriter(fw, 10)
	if err != nil {
		log.Fatal(err)
	}

	println(args.Index)
	bar := pb.StartNew(result.Size)

	for doc := range result.Docs {
		writer.Append(doc)
		bar.Increment()
	}
	writer.Close()
	println("\nDONE!")
}

func buildFileWriter(args Args) (io.WriteCloser, error) {
	file := args.Index + ".json"
	if !args.Compress {
		return os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0666)
	}
	f, err := os.OpenFile(args.Index+".gz", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	gw, err := gzip.NewWriterLevel(f, gzip.BestCompression)
	if err != nil {
		return nil, err
	}
	gw.Name = file
	return gw, nil
}
