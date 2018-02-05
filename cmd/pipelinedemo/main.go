package main

import (
	"bufio"
	"os"
	"fmt"
	"github.com/Joursion/sort/pipeline"
)


func main(){
	const filename = "large.in"
	const N = 20000000

	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	p := pipeline.RandomSource(N)
	writer := bufio.NewWriter(file)
	pipeline.WriterSink(writer, p)
	writer.Flush()
	file, err = os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	p = pipeline.ReadSource(bufio.NewReader(file), -1)

	count := 0
	for v := range p {
		fmt.Println(v)
		count ++
		if count >= 100 {
			break
		}
	}
}

func mergeDemo () {
	p := pipeline.Merge(
		pipeline.InMemSort(
			pipeline.ArraySource(
				3, 2, 6, 7, 4)),
		pipeline.InMemSort(
			pipeline.ArraySource(
				44, 22, 65, 7, 4)),
	)
	for {
		num, ok := <- p
		if ok {
			fmt.Println(num)
		} else {
			break
		}
	}
	// 如果使用 range ，发送方一定要 close
	// for v := range p {
	// 	fmt.Println(v)
	// }
}