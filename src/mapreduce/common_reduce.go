package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {

	intermediateFileName := reduceName(jobName, nMap, reduceTask)
	fileName := "/Users/xiao/PRJOFMIT/intermediates/" + intermediateFileName + ".json"

	var finalRes string

	if mapFilePtr, errOnOpen := os.Open(fileName); errOnOpen == nil {

		// fmt.Println("reducefile" + fileName + ": start reading")

		decoder := json.NewDecoder(mapFilePtr)

		var kvarr []KeyValue

		for i := 0; decoder.More(); i++ {

			var tmp KeyValue
			errOnDecode := decoder.Decode(&tmp)

			if errOnDecode != nil {
				fmt.Println("解码失败=", errOnDecode)
			}
			kvarr[i] = tmp
		}

		// sort by key
		sort.Sort(ByKey(kvarr))

		var vValues []string
		var inputKey string

		//after sort collect values of a key
		for index, kv := range kvarr {
			if inputKey == "" {
				inputKey = kv.Key
			}
			vValues[index] = kv.Value
		}

		//invoke reduceF
		finalRes = reduceF(inputKey, vValues)

		// write return value of reduceF to output file
		mapFilePtr.Close()
		// fmt.Println("reducefile" + fileName + " :read over")
	}

	ResultFilePtr, errOnCreate := os.Create(outFile)

	if errOnCreate != nil {
		fmt.Println("创建文件失败，err=", errOnCreate)
		return
	}

	encoder := json.NewEncoder(ResultFilePtr)

	errOnEncode := encoder.Encode(&finalRes)

	if errOnEncode != nil {
		fmt.Println("编码失败，err=", errOnEncode)
	} else {
		// fmt.Println("编码成功")
	}
	ResultFilePtr.Close()
	fmt.Println("finalRes: " + outFile + ": writed over")

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
