package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"unsafe"
)

func BytesToString(b []byte) string {
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh := reflect.StringHeader{bh.Data, bh.Len}
	return *(*string)(unsafe.Pointer(&sh))
}

// 分区和排序
// Partitioner的作用：
// 对map端输出的数据key作一个散列，使数据能够均匀分布在各个reduce上进行后续操作，避免产生热点区。

//(Partition)分区出现的必要性，如何使用Hadoop产生一个全局排序的文件？
// 最简单的方法就是使用一个分区，但是该方法在处理大型文件时效率极低，
//因为一台机器必须处理所有输出文件，从而完全丧失了MapReduce所提供的并行架构的优势。
//事实上我们可以这样做，首先创建一系列排好序的文件；其次，串联这些文件（类似于归并排序）；
//最后得到一个全局有序的文件。主要的思路是使用一个partitioner来描述全局排序的输出。
//比方说我们有1000个1-10000的数据，跑10个ruduce任务，
//如果我们运行进行partition的时候，能够将在1-1000中数据的分配到第一个reduce中，1001-2000的数据分配到第二个reduce中，以此类推。
//即第n个reduce所分配到的数据全部大于第n-1个reduce中的数据。
//这样，每个reduce出来之后都是有序的了，我们只要cat所有的输出文件，变成一个大的文件，就都是有序的了

// 它以key的Hash值对Reducer的数目取模，得到对应的Reducer。这样保证如果有相同的key值，
// 肯定被分配到同一个reducre上。如果有N个reducer，编号就为0,1,2,3……(N-1)。

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string, //this just is a file name!
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {

	//Step1 first read the file and transfer it to users function mapF
	//The map function is called once for each file of input

	contents, errOnReadFile := ioutil.ReadFile(inFile)
	if errOnReadFile != nil {
		log.Printf("read file %s failed", inFile)
	}

	contents_result := BytesToString(contents)
	//open input file read and  send file contents to map
	mapResKeyValueArr := mapF(inFile, contents_result)

	//store encorder
	var encorders = make([]*json.Encoder, nReduce)

	//create files
	for r := 0; r < nReduce; r++ {
		intermediateFileName := reduceName(jobName, mapTask, r)

		filePtr, errOnCreate := os.Create(intermediateFileName)
		defer filePtr.Close()

		if errOnCreate != nil {
			fmt.Println("创建文件失败，err=", errOnCreate)
			return
		}
		encorders[r] = json.NewEncoder(filePtr)
	}

	//reduceTaskInt 和encorders 中文件句柄的下标记是对应的
	for _, kv := range mapResKeyValueArr {
		reduceTaskInt := ihash(kv.Key) % nReduce

		errOnEncode := encorders[reduceTaskInt].Encode(&kv)

		if errOnEncode != nil {
			fmt.Println("编码失败，err=", errOnEncode)
		} else {
			// fmt.Println("编码成功")
		}
	}

	//Call ihash() (see
	// below) on each key, mod nReduce, to pick r for a key/value pair.

	//
	// doMap manages one map task: it should read one of the input files
	// (inFile), call the user-defined map function (mapF) for that file's
	// contents, and partition mapF's output into nReduce intermediate files.
	//
	// There is one intermediate file per reduce task. The file name
	// includes both the map task number and the reduce task number. Use
	// the filename generated by reduceName(jobName, mapTask, r)
	// as the intermediate file for reduce task r. Call ihash() (see
	// below) on each key, mod nReduce, to pick r for a key/value pair.
	//
	// mapF() is the map function provided by the application. The first
	// argument should be the input file name, though the map function
	// typically ignores it. The second argument should be the entire
	// input file contents. mapF() returns a slice containing the
	// key/value pairs for reduce; see common.go for the definition of
	// KeyValue.
	//
	// Look at Go's ioutil and os packages for functions to read
	// and write files.
	//
	// Coming up with a scheme for how to format the key/value pairs on
	// disk can be tricky, especially when taking into account that both
	// keys and values could contain newlines, quotes, and any other
	// character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	//
	// Your code here (Part I).
	//
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
