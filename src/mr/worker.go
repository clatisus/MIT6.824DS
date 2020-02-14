package mr

import (
	"fmt"
	"log"
	"net/rpc"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

func askForTask(args AskForTaskArgs) (reply AskForTaskReply, ok bool) {
	ok = call("Master.GetTask", &args, &reply)
	return
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	args := AskForTaskArgs{}
	args.CompleteTask.Phase = UndefinedPhase
	for {
		reply, ok := askForTask(args)
		if !ok || reply.Done {
			break
		}
		task := &reply.Task
		args.CompleteTask = *task

		switch task.Phase {
		case MapPhase:
			mapTask := task.MapTask
			Map(mapTask.FileName, mapTask.MapIndex, mapTask.ReduceNumber, mapf)
		case ReducePhase:
			reduceTask := task.ReduceTask
			Reduce(reduceTask.ReduceIndex, reduceTask.MapNumber, reducef)
		default:
			log.Fatalf("unknown task phase: %v", task.Phase)
		}
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
