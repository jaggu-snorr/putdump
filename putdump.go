package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"runtime"
	"sync"

	"github.com/cheggaaa/pb/v3"
)

type options struct {
	dumpFile string
	url      string
	index    string
	verbose  bool
	bulkSize int
	parallel int
}

type bulkRowPair struct {
	id  string
	doc string
}

func getOptions() options {
	var options options

	flag.StringVar(&options.dumpFile, "dump-file", "", "Dumpfile in ndjson format")
	flag.StringVar(&options.url, "url", "", "Elasticsearch url, eg: http://localhost:9200")
	flag.StringVar(&options.index, "index", "", "Elasticsearch index")
	flag.BoolVar(&options.verbose, "verbose", false, "Verbose output, defaults to false")
	flag.IntVar(&options.bulkSize, "bulk-size", 1000, "Number of documents in each bulk request, defaults to 1000")
	flag.IntVar(&options.parallel, "parallel", runtime.NumCPU(), "Number of parallel bulk posts, defaults to 2x number of cores")

	flag.Parse()

	if options.dumpFile == "" || options.url == "" || options.index == "" {
		flag.Usage()
		os.Exit(1)
	}

	return options
}

func postBulk(bulkBuffer []byte, errors chan<- string, options options) {
	url := fmt.Sprintf("%s/%s/_bulk", options.url, options.index)
	resp, err := http.Post(url, "application/x-ndjson", bytes.NewReader(bulkBuffer))
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	if options.verbose {
		body, _ := ioutil.ReadAll(resp.Body)
		fmt.Println(string(body))
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		errors <- fmt.Sprintf("Error: %s, Response: %s", resp.Status, string(body))
	}
}

func putDump(options options, errors chan<- string, done chan<- bool) {
	jobs := make(chan []byte, options.parallel)
	var wg sync.WaitGroup

	for i := 0; i < options.parallel; i++ {
		go func() {
			wg.Add(1)

			for {
				select {
				case buffer, ok := <-jobs:
					if !ok {
						wg.Done()
						return
					}

					postBulk(buffer, errors, options)
				}
			}
		}()
	}

	bar := pb.Simple.Start64(getFileSize(options.dumpFile))
	processFile(options, jobs, bar)

	wg.Wait()
	bar.Finish()
	done <- true
}

func processFile(options options, jobs chan []byte, bar *pb.ProgressBar) {
	file, err := os.Open(options.dumpFile)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()

	bulkBuffer := make([]byte, 0)
	barReader := bar.NewProxyReader(file)
	scanner := bufio.NewScanner(barReader)
	buf := make([]byte, 0, 64*1024) // Buffer with 64KB initial size
	scanner.Buffer(buf, 1024*1024)  // Max 1MB lines
	count := 0
	for scanner.Scan() {
		count = count + 1

		idRow := scanner.Text() + "\n"
		bulkBuffer = append(bulkBuffer, idRow...)

		if !scanner.Scan() {
			log.Fatal("Not an even number of rows in dumpfile? idRow: ", idRow, ", scanner error: ", scanner.Err())
		}

		docRow := scanner.Text() + "\n"
		bulkBuffer = append(bulkBuffer, docRow...)

		if count == options.bulkSize {
			jobs <- bulkBuffer

			count = 0
			bulkBuffer = make([]byte, 0)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatalln(err)
	}

	if count > 0 {
		jobs <- bulkBuffer
	}

	close(jobs)
}

func getFileSize(filename string) int64 {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatalln(err)
	}

	return fileInfo.Size()
}

func main() {
	options := getOptions()

	done := make(chan bool)
	errors := make(chan string)

	go putDump(options, errors, done)

	for {
		select {
		case err := <-errors:
			fmt.Println("Error: ", err)
		case <-done:
			return
		}
	}
}
