package mr

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
	Phase      jobPhase
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

type jobPhase string

const (
	mapPhase       jobPhase = "map"
	reducePhase    jobPhase = "reduce"
	undefinedPhase jobPhase = "undefined"
)
