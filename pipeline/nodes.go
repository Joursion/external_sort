package pipeline

import (
	"fmt"
	"time"
	"encoding/binary"
	"io"
	"sort"
	"math/rand"
)

var startTime time.Time

func Init() {
	startTime = time.Now()
}

//
func ArraySource(a ...int) <-chan int {
	out := make(chan int)
	go func() {
		for _, v := range a {
			out <- v
		}
		close(out)  // 明显的结束，所以添加 close
	}()
	return out
}

func InMemSort(in <-chan int) <-chan int {
	out := make(chan int, 1024)
	go func() {
		// Read into memory
		a := []int{}
		for v := range in {
			a = append(a, v)
		}
		fmt.Println("Read done: ", time.Now().Sub(startTime))
		sort.Ints(a)
		fmt.Println("InMemSort done: ", time.Now().Sub(startTime))
		for _, v := range a{
			out <- v
		}
		close(out)
	}()
	return out
}

func Merge(in1, in2 <-chan int) <-chan int{
	out := make(chan int)
	go func() {
		v1, ok1 := <- in1
		v2, ok2 := <- in2
		for ok1 || ok2 {
			if !ok2  || (ok1 && v1 <= v2){
				out <- v1
				v1, ok1 = <- in1
			} else {
				out <- v2
				v2, ok2 = <- in2
			}
		}
		close(out)
		fmt.Println("Merge done: ", time.Now().Sub(startTime))
	}()
	return out
}

func ReadSource(reader io.Reader, chunkSize int) <-chan int {
	out := make(chan int, 1024)
	go func() {
		buffer := make([]byte, 8)
		bytesRead := 0
		for {
			n, err := reader.Read(buffer) // 有可能是 err = EOF, 或者 n 小于 8字节
			if n > 0 {
				v := int(binary.BigEndian.Uint64(buffer))
				bytesRead += n
				out <- v
			}
			if err != nil  || (chunkSize != -1 && bytesRead >= chunkSize) {
				break
			}
		}
		close(out)
	}()
	return out
}

func WriterSink(writer io.Writer, in <-chan int) {
	for v := range in{
		buffer := make([]byte, 8)
		binary.BigEndian.PutUint64(buffer, uint64(v))
		writer.Write(buffer)
	}
}

func RandomSource(count int) <-chan int {
	out := make(chan int)
	go func(){
		for i:= 0; i < count; i++{
			out <- rand.Int()
		}
		close(out)
	}()
	return out
}

// 多路两两归并
func MergeN(inputs ...<-chan int) <-chan int {
	if (len(inputs) == 1) {
		return inputs[0]
	}
	m := len(inputs) / 2
	// inputs[0: m) inputs[m: len]
	return Merge(MergeN(inputs[:m]...), MergeN(inputs[m:]...))
}