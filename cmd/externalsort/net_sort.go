package main

import (
	"strconv"
	"fmt"
	"bufio"
	"github.com/Joursion/sort/pipeline"
	"os"
)

func main() {
	const filename = "small.in"
	const outFilename = "small.out"
	p := createNetworkPipeline(
		filename, 512, 4,
	)

	// const filename = "large.in"
	// const outFilename = "large.out"
	// p := createNetworkPipeline(
	// 	filename, 160000000, 4,
	// )
	writeToFile(p, outFilename)
	printFile(outFilename)
}

func writeToFile(p <-chan int, filename string) {
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	defer writer.Flush()

	pipeline.WriterSink(writer, p)
}

func printFile(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	p := pipeline.ReadSource(file, -1)
	count := 0
	for v := range p {
		fmt.Println(v)
		count ++
		if count >= 100 {
			break
		}
	}
}

func createPipeline(filename string, fileSize, chunkCount int) <-chan int {
	chunkSize := fileSize / chunkCount
	pipeline.Init()
	sortResults := []<-chan int{}
	for i := 0; i < chunkCount; i++{
		file, err := os.Open(filename)
		if err != nil {
			panic(err)
		}
		
		file.Seek(int64(i * chunkSize), 0)

		source := pipeline.ReadSource(bufio.NewReader(file), chunkSize)

		sortResults = append(sortResults, pipeline.InMemSort(source))

	}
	return pipeline.MergeN(sortResults...)
}

func createNetworkPipeline(filename string, fileSize, chunkCount int) <-chan int {
	chunkSize := fileSize / chunkCount
	pipeline.Init()
	sortResults := []<-chan int{}
	sortAddr := []string{}
	for i := 0; i < chunkCount; i++{
		file, err := os.Open(filename)
		if err != nil {
			panic(err)
		}
		
		file.Seek(int64(i * chunkSize), 0)

		source := pipeline.ReadSource(bufio.NewReader(file), chunkSize)

		// sortResults = append(sortResults, pipeline.InMemSort(source))
		addr := ":" + strconv.Itoa(7000 + i)
		pipeline.NetworkSink(addr, pipeline.InMemSort(source))
		sortAddr = append(sortAddr, addr)
	}
	// sortResults := []<-chan int{}
	for _, addr := range sortAddr {
		sortResults = append(sortResults, pipeline.NetworkSource(addr))
	}
	return pipeline.MergeN(sortResults...)
}