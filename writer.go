package main

import (
	"bufio"
	"log"
	"os"
)

type DumpWriter struct {
	f    *os.File
	docs chan string
}

func NewWriter(path string, bufSize int) (*DumpWriter, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	docs := make(chan string, bufSize)

	go func() {
		writer := bufio.NewWriter(f)
		defer func() {
			writer.Flush()
			f.Close()
		}()
		for doc := range docs {
			_, err := writer.WriteString(doc)
			_, err = writer.WriteString("\n")
			if err != nil {
				log.Println("write file err: " + err.Error())
			}
		}

	}()

	return &DumpWriter{
		f:    f,
		docs: docs,
	}, nil
}

func (w DumpWriter) Append(doc string) {
	w.docs <- doc
}

func (w DumpWriter) Close() {
	close(w.docs)
}
