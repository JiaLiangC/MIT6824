package mapreduce

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
)

// type ByKey []KeyValue

// func (a ByKey) Len() int           { return len(a) }
// func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
// func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {

	//每个 map 都产生了特定reduce 的文件，所以reduce需要去拉取到所有的这些map产生的文件
	// 每个map产生了n个文件（为每个reduce产生一个文件），所有的map总共产生了m x n个文件，
	// 相同的key的数据都会被打到这个map下的同一个reduce的文件中去
	//所以reduce 想要得到这个key的所有数据，需要去拉取所有map的属于他的文件

	//第一步分别拉取 n个map 的任务，每拉取到一个map中对应于该reduce的文件，就读取文件内容，
	//然后对该文件的内容中的key  组织起来 list{key1: [values],key1: [values]}
	//最后list中的每个key使用用户传入的reduce 函数，并把用户reduce函数的输出结果写到输出文件

	kvs := make(map[string][]string)

	for m := 0; m < nMap; m++ {
		mapFilePtr, errOnOpen := os.Open(reduceName(jobName, m, reduceTask))
		fmt.Println(reduceName(jobName, m, reduceTask))

		if errOnOpen != nil {
			log.Fatal("doReduce: ", errOnOpen)
		}

		decoder := json.NewDecoder(mapFilePtr)

		// decode map intermediate file content to kvs
		for {
			var tmp KeyValue
			errOnDecode := decoder.Decode(&tmp)

			if errOnDecode != nil {
				fmt.Println("解码失败=", errOnDecode)
				break
			}
			_, ok := kvs[tmp.Key]

			if !ok {
				kvs[tmp.Key] = []string{}
			}

			kvs[tmp.Key] = append(kvs[tmp.Key], tmp.Value)
		}

		// write return value of reduceF to output file
		mapFilePtr.Close()
	}

	//collect and sort keys
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// fmt.Println(keys)

	ResultFilePtr, errOnCreate := os.Create(outFile)
	if errOnCreate != nil {
		fmt.Println("创建文件失败，err=", errOnCreate)
		return
	}
	encoder := json.NewEncoder(ResultFilePtr)

	//invoke reduceF per distinct key
	for _, key := range keys {

		finalRes := reduceF(key, kvs[key])

		errOnEncode := encoder.Encode(KeyValue{key, finalRes})
		if errOnEncode != nil {
			fmt.Println("编码失败，err=", errOnEncode)
		}
	}

	ResultFilePtr.Close()

	// fmt.Println("finalRes: " + outFile + ": writed over")

	//and write into corresponding file

	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
}
