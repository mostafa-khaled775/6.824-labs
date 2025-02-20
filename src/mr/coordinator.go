package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	state CoordinatorState
	lock  sync.Mutex
}

func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.state.finished()
}

func (c *Coordinator) PollTask(args *PollTaskArgs, reply *PollTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.state.finished() {
		reply.Task.Done = true
		return nil
	}

	t := c.state.pollTask()

	go func() {
		time.Sleep(10 * time.Second)
		c.lock.Lock()
		defer c.lock.Unlock()
		c.state.insertTask(t)
	}()

	reply.Task = t
	return nil
}

func (s *CoordinatorState) insertTask(t MRTask) {
	s.idleTasks = append(s.idleTasks, t)
}

func (co *Coordinator) HandleTaskResults(args *HandleTaskResultsArgs, reply *HandleTaskResultsReply) error {
	co.lock.Lock()
	defer co.lock.Unlock()
	co.state.handleTaskResults(args.Task, args.Results)
	return nil
}

type CoordinatorState struct {
	iFiles        [][]string
	nMap          int
	nReduce       int
	finishedTasks map[MRTaskId]struct{}
	idleTasks     []MRTask
	quit          chan struct{}
}

func (s *CoordinatorState) isTaskExecuted(t MRTask) bool {
	_, ok := s.finishedTasks[t.Id()]
	return ok
}

func (s *CoordinatorState) pollTask() MRTask {
	if len(s.idleTasks) == 0 {
		// signal the worker to wait
		return MRTask{}
	}
	task := s.idleTasks[0]
	s.idleTasks = s.idleTasks[1:]
	if s.isTaskExecuted(task) {
		return s.pollTask()
	}
	return task
}

func (c *CoordinatorState) finished() bool {
	return len(c.finishedTasks) == c.nMap+c.nReduce
}

func (s *CoordinatorState) handleTaskResults(t MRTask, results []string) {
	if _, ok := s.finishedTasks[t.Id()]; ok {
		return
	}
	s.finishedTasks[t.Id()] = struct{}{}
	if t.MapTask != nil {
		s.handleMapTaskResults(*t.MapTask, results)
	} else if t.ReduceTask != nil {
		s.handleReduceTaskResults(*t.ReduceTask, results)
	} else {
		log.Fatalf("Invalid task type: %v", t)
	}
}

func (co *CoordinatorState) handleMapTaskResults(_ MRMapTask, results []string) {
	co.iFiles = append(co.iFiles, results)

	if len(co.finishedTasks) == co.nMap {
		for i := 0; i < co.nReduce; i++ {
			iFiles := make([]string, 0)
			for _, files := range co.iFiles {
				iFiles = append(iFiles, files[i])
			}
			t := makeReduceTask(i, iFiles, fmt.Sprintf("mr-out-%d", i))
			co.idleTasks = append(co.idleTasks, t)
		}
	}
}

func (co *CoordinatorState) handleReduceTaskResults(_ MRReduceTask, _ []string) {
	if co.finished() {
		close(co.quit)
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func MakeCoordinatorState(files []string, nReduce int) *CoordinatorState {
	mTasks := make([]MRTask, len(files))
	for i, file := range files {
		mTasks[i] = makeMapTask(file, nReduce, i)
	}
	s := &CoordinatorState{
		iFiles:        make([][]string, 0),
		nMap:          len(files),
		nReduce:       nReduce,
		idleTasks:     mTasks,
		finishedTasks: make(map[MRTaskId]struct{}),
		quit:          make(chan struct{}),
	}
	return s
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{state: *MakeCoordinatorState(files, nReduce)}

	c.server()
	return &c
}
