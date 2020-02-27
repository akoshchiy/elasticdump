package main

import (
	"bufio"
	"io"
	"log"
)

type DumpWriter struct {
	w    io.WriteCloser
	docs chan string
}

func NewDumpWriter(w io.WriteCloser, bufSize int) (*DumpWriter, error) {
	docs := make(chan string, bufSize)

	go func() {
		writer := bufio.NewWriter(w)
		defer func() {
			_ = writer.Flush()
			_ = w.Close()
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
		w:    w,
		docs: docs,
	}, nil
}

func (w DumpWriter) Append(doc string) {
	w.docs <- doc
}

func (w DumpWriter) Close() {
	close(w.docs)
}
