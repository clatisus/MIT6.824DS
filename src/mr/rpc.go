package mr

import (
	"os"
	"strconv"
)

//
// RPC definitions.
//

type MapTask struct {
	FileName     string
	MapIndex     int
	ReduceNumber int
}

type ReduceTask struct {
	ReduceIndex int
	MapNumber   int
}

type Task struct {
	Phase      JobPhase
	MapTask    MapTask
	ReduceTask ReduceTask
}

type AskForTaskArgs struct {
	CompleteTask Task
}

type AskForTaskReply struct {
	Task Task
	Done bool
}

type JobPhase string

const (
	MapPhase       JobPhase = "map"
	ReducePhase    JobPhase = "reduce"
	UndefinedPhase JobPhase = "undefined"
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
