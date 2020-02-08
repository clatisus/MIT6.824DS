package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
)

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func getIntermediateFileName(mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("mr-%d-%d", mapIndex, reduceIndex)
}

func Map(fileName string, taskIndex int, nReduce int, mapF func(string, string) []KeyValue) {
	content, err := ioutil.ReadFile(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}

	kva := mapF(fileName, string(content))

	output := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		indexReduce := ihash(kv.Key) % nReduce
		output[indexReduce] = append(output[indexReduce], kv)
	}

	for reduceNumber, kvs := range output {
		intermediateFileName := getIntermediateFileName(taskIndex, reduceNumber)
		f, err := ioutil.TempFile("", "map_temp")
		if err != nil {
			log.Printf("Error: %v", err)
		}
		enc := json.NewEncoder(f)
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				log.Printf("Error: %v", err)
			}
		}
		f.Close()
		os.Rename(f.Name(), intermediateFileName)
	}
}
