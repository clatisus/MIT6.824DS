package main

import (
	"fmt"
	"github.com/clatisus/MIT6.824DS/src/mr"
	"log"
	"os"
	"plugin"
	"strconv"
	"sync"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: runmr xxx.so reduce# inputfiles...\n")
		os.Exit(1)
	}
	mapf, reducef := loadPlugin(os.Args[1])
	nReduce, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	var done sync.WaitGroup
	for i, filename := range os.Args[3:] {
		done.Add(1)
		go func(i int, filename string) {
			defer done.Done()
			mr.Map(filename, i, nReduce, mapf)
		}(i, filename)
	}
	done.Wait()

	nMap := len(os.Args[3:])
	for i := 0; i < nReduce; i++ {
		done.Add(1)
		go func(i int) {
			defer done.Done()
			mr.Reduce(i, nMap, reducef)
		}(i)
	}
	done.Wait()
}

//
// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
//
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
