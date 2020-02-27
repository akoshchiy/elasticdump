package main

import (
	"encoding/json"
	"errors"
	"fmt"
	es "github.com/elastic/go-elasticsearch/v5"
	"github.com/elastic/go-elasticsearch/v5/esapi"
	"io/ioutil"
	"log"
	"sync"
	"time"
)

type Dumper struct {
	client *es.Client
	pool   *Pool
}

func NewDumper(client *es.Client, pool *Pool) Dumper {
	return Dumper{client: client, pool: pool}
}

type DumpResult struct {
	Size int
	Docs <-chan string
}

func (d Dumper) Dump(args Args) (DumpResult, error) {
	shards, err := d.queryShardsCount(args.Index)
	if err != nil {
		return DumpResult{}, err
	}
	count, err := d.queryDocsCount(args.Index)
	if err != nil {
		return DumpResult{}, err
	}

	docs := make(chan string)

	result := DumpResult{
		Size: count,
		Docs: docs,
	}

	if count == 0 {
		close(docs)
		return result, nil
	}

	var wg sync.WaitGroup
	wg.Add(shards)

	go func() {
		for i := 0; i < shards; i++ {
			go func(shard int) {
				defer wg.Done()
				d.queryShard(args, shard, docs)
			}(i)
		}
		wg.Wait()
		close(docs)
	}()

	return result, nil
}

func (d Dumper) queryShard(args Args, shard int, ch chan string) {
	res := <-d.pool.Submit(func() ScrollResult { return d.queryFirst(args, shard) })

	if res.Err != nil {
		log.Println("failed to query shard: " + res.Err.Error())
		return
	}
	if len(res.Hits) == 0 {
		return
	}
	for _, hit := range res.Hits {
		ch <- hit
	}
	for {
		res = <-d.pool.Submit(func() ScrollResult { return d.queryScroll(res.ScrollId) })
		if len(res.Hits) == 0 {
			break
		}
		if res.Err != nil {
			log.Println("failed to query shard: " + res.Err.Error())
			break
		}
		for _, hit := range res.Hits {
			ch <- hit
		}
	}
}

func (d Dumper) queryFirst(args Args, shard int) ScrollResult {
	search := d.client.Search
	preference := fmt.Sprintf("_shards:%d", shard)
	resp, err := search(
		search.WithIndex(args.Index),
		search.WithDocumentType(args.Type),
		search.WithSort("_doc"),
		search.WithSize(args.BatchSize),
		search.WithPreference(preference),
		search.WithScroll(time.Minute),
	)
	if err != nil {
		return ScrollResult{ScrollId: "", Hits: nil, Err: err}
	}
	doc, err := d.decodeBody(resp)
	if err != nil {
		return ScrollResult{ScrollId: "", Hits: nil, Err: err}
	}

	scrollId := doc["_scroll_id"].(string)
	hits := d.extractHits(doc)

	return ScrollResult{ScrollId: scrollId, Hits: hits, Err: nil}
}

func (d Dumper) queryScroll(scrollId string) ScrollResult {
	scroll := d.client.Scroll
	resp, err := scroll(
		scroll.WithScrollID(scrollId),
		scroll.WithScroll(time.Minute),
	)
	if err != nil {
		return ScrollResult{ScrollId: "", Hits: nil, Err: err}
	}
	doc, err := d.decodeBody(resp)
	if err != nil {
		return ScrollResult{ScrollId: "", Hits: nil, Err: err}
	}

	newScrollId := doc["_scroll_id"].(string)
	hits := d.extractHits(doc)

	return ScrollResult{ScrollId: newScrollId, Hits: hits, Err: err}
}

func (d Dumper) extractHits(doc map[string]interface{}) []string {
	result := make([]string, 0)
	hits := doc["hits"].(map[string]interface{})["hits"].([]interface{})
	for _, hit := range hits {
		source := hit.(map[string]interface{})["_source"].(map[string]interface{})
		bytes, _ := json.Marshal(source)
		result = append(result, string(bytes))
	}
	return result
}

func (d Dumper) queryDocsCount(index string) (int, error) {
	count := d.client.Count
	resp, err := count(count.WithIndex(index))
	if err != nil {
		return 0, err
	}
	doc, err := d.decodeBody(resp)
	if err != nil {
		return 0, err
	}
	return int(doc["count"].(float64)), nil
}

func (d Dumper) queryShardsCount(index string) (int, error) {
	search := d.client.SearchShards
	resp, err := search(search.WithIndex(index))
	if err != nil {
		return 0, err
	}
	doc, err := d.decodeBody(resp)
	if err != nil {
		return 0, err
	}
	return len(doc["shards"].([]interface{})), nil
}

func (d Dumper) decodeBody(resp *esapi.Response) (doc map[string]interface{}, err error) {
	if resp.IsError() {
		body, _ := ioutil.ReadAll(resp.Body)
		return nil, errors.New(string(body))
	}
	decoder := json.NewDecoder(resp.Body)
	doc = map[string]interface{}{}
	err = decoder.Decode(&doc)
	return
}

type Job func() ScrollResult

type ScrollResult struct {
	ScrollId string
	Hits     []string
	Err      error
}

type Pool struct {
	queue chan func()
}

func NewPool(workerCount int, bufSize int) *Pool {
	jobs := make(chan func(), bufSize)
	for i := 0; i < workerCount; i++ {
		go func() {
			for job := range jobs {
				job()
			}
		}()
	}
	return &Pool{queue: jobs}
}

func (p *Pool) Submit(job Job) <-chan ScrollResult {
	out := make(chan ScrollResult, 1)
	p.queue <- func() {
		defer close(out)
		out <- job()
	}
	return out
}
