package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Master struct
type Master struct {
	sync.Mutex
	cond *sync.Cond

	files          []string
	nMap           int
	nReduce        int
	finishedMap    int
	finishedReduce int

	mapTaskChan    chan int
	reduceTaskChan chan int

	runningMapTaskMap    map[int]int64
	runningReduceTaskMap map[int]int64
}

type ScheduleResult string

const (
	Success     ScheduleResult = "success"
	NoAvailable ScheduleResult = "noAvailable"
	Done        ScheduleResult = "done"
)

func (m *Master) GetTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	m.Lock()
	defer m.Unlock()

	m.finishTask(args.CompleteTask)
	for {
		var result ScheduleResult
		reply.Task, result = m.scheduleTask()
		reply.Done = false
		switch result {
		case Success:
			return nil
		case Done:
			reply.Done = true
			return nil
		default: // NoAvailable
			m.cond.Wait()
		}
	}
}

func (m *Master) finishTask(task Task) {
	// mark piggybacked task as completed
	// if undefined phase, do not broadcast
	switch task.Phase {
	case MapPhase:
		_, ok := m.runningMapTaskMap[task.MapTask.MapIndex]
		if !ok {
			// task may be finished multiple times due to timeout re-schedule
			return
		}
		delete(m.runningMapTaskMap, task.MapTask.MapIndex)
		m.finishedMap++
		m.cond.Broadcast()
	case ReducePhase:
		_, ok := m.runningReduceTaskMap[task.ReduceTask.ReduceIndex]
		if !ok {
			return
		}
		delete(m.runningReduceTaskMap, task.ReduceTask.ReduceIndex)
		m.finishedReduce++
		m.cond.Broadcast()
	}
}

func (m *Master) scheduleTask() (task Task, result ScheduleResult) {
	now := time.Now().Unix()

	select {
	case mapIndex := <-m.mapTaskChan:
		task.Phase = MapPhase
		task.MapTask = MapTask{
			FileName:     m.files[mapIndex],
			MapIndex:     mapIndex,
			ReduceNumber: m.nReduce,
		}
		m.runningMapTaskMap[mapIndex] = now
		return task, Success
	default:
		if len(m.runningMapTaskMap) > 0 {
			return task, NoAvailable
		}
	}

	select {
	case reduceIndex := <-m.reduceTaskChan:
		task.Phase = ReducePhase
		task.ReduceTask = ReduceTask{
			ReduceIndex: reduceIndex,
			MapNumber:   m.nMap,
		}
		m.runningReduceTaskMap[reduceIndex] = now
		return task, Success
	default:
		if len(m.runningReduceTaskMap) > 0 {
			return task, NoAvailable
		}
	}

	return task, Done
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.Lock()
	defer m.Unlock()

	return m.finishedMap == m.nMap && m.finishedReduce == m.nReduce
}

func (m *Master) tick() {
	const TIMEOUT = 10
	for {
		if m.Done() {
			return
		}
		m.Lock()

		rescheduled := false
		now := time.Now().Unix()
		for mapIndex, startTime := range m.runningMapTaskMap {
			if startTime+TIMEOUT < now {
				delete(m.runningMapTaskMap, mapIndex)
				m.mapTaskChan <- mapIndex
				rescheduled = true
			}
		}
		for reduceIndex, startTime := range m.runningReduceTaskMap {
			if startTime+TIMEOUT < now {
				delete(m.runningReduceTaskMap, reduceIndex)
				m.reduceTaskChan <- reduceIndex
				rescheduled = true
			}
		}

		if rescheduled {
			m.cond.Broadcast()
		}

		m.Unlock()
		time.Sleep(time.Second)
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.cond = sync.NewCond(&m)

	m.files = files
	m.nMap = len(files)
	m.nReduce = nReduce
	m.finishedMap = 0
	m.finishedReduce = 0

	m.mapTaskChan = make(chan int, m.nMap)
	m.reduceTaskChan = make(chan int, m.nReduce)

	m.runningMapTaskMap = make(map[int]int64)
	m.runningReduceTaskMap = make(map[int]int64)

	for i := 0; i < m.nMap; i++ {
		m.mapTaskChan <- i
	}
	for i := 0; i < m.nReduce; i++ {
		m.reduceTaskChan <- i
	}

	m.server()
	go m.tick()
	return &m
}
