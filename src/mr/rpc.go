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
