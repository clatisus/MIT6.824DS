package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

func getOutputFileName(taskIndex int) string {
	return fmt.Sprintf("mr-out-%d", taskIndex)
}

func Reduce(taskIndex int, nMap int, reduceF func(string, []string) string) {
	keyValuesMap := make(map[string][]string)

	for i := 0; i < nMap; i++ {
		intermediateFileName := getIntermediateFileName(i, taskIndex)
		f, err := os.Open(intermediateFileName)
		if err != nil {
			log.Fatalf("cannot open %v", intermediateFileName)
		}

		dec := json.NewDecoder(f)
		for dec.More() {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				log.Fatalf("Error: %v", err)
			}
			if values, ok := keyValuesMap[kv.Key]; ok {
				keyValuesMap[kv.Key] = append(values, kv.Value)
			} else {
				keyValuesMap[kv.Key] = []string{kv.Value}
			}
		}
		f.Close()
	}

	outputFileName := getOutputFileName(taskIndex)
	f, _ := ioutil.TempFile("", "reduce_temp")
	for k, v := range keyValuesMap {
		output := reduceF(k, v)
		fmt.Fprintf(f, "%v %v\n", k, output)
	}
	f.Close()
	os.Rename(f.Name(), outputFileName)
}
