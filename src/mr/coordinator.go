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

const (
	TIMEOUT = 10
)

type Task struct {
	FileName  string
	Finished  bool
	StartTime time.Time
}

type SharedState struct {
	mu              sync.Mutex
	MapTasks        []Task
	ReduceTasks     []Task
	MapPhaseDone    bool
	ReducePhaseDone bool
}
type Coordinator struct {
	// Your definitions here.
	// MapTaskCount int
	NReduce     int
	SharedState *SharedState
}

func (c *Coordinator) TaskFinishedHandler(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.SharedState.mu.Lock()
	defer c.SharedState.mu.Unlock()

	taskID := args.TaskID
	if args.IsMapTask {
		c.SharedState.MapTasks[taskID].Finished = true
	} else {
		c.SharedState.ReduceTasks[taskID].Finished = true
	}

	return nil
}

func (c *Coordinator) FindNextTaskHandler(args *GetTaskArgs, reply *GetTaskReply) error {
	c.SharedState.mu.Lock()
	defer c.SharedState.mu.Unlock()

	reply.NReduce = c.NReduce
	reply.NMap = len(c.SharedState.MapTasks)
	reply.PleaseExit = false

	taskID := -1
	var fileName string

	if !c.SharedState.MapPhaseDone {
		c.SharedState.MapPhaseDone = true

		for i, task := range c.SharedState.MapTasks {
			now := time.Now()
			if !task.Finished {
				c.SharedState.MapPhaseDone = false

				if now.Sub(task.StartTime).Seconds() > TIMEOUT {
					c.SharedState.MapTasks[i].StartTime = now

					fileName = c.SharedState.MapTasks[i].FileName
					taskID = i
					break
				}
			}
		}

		if taskID != -1 {
			reply.TaskID = taskID
			reply.FileName = fileName
			reply.IsMapTask = true

			// log.Default().Printf("Assign map task with id: %d, fileName: %s", taskID, fileName)
			return nil
		}
	}

	if c.SharedState.MapPhaseDone && !c.SharedState.ReducePhaseDone {
		c.SharedState.ReducePhaseDone = true

		for i, task := range c.SharedState.ReduceTasks {
			now := time.Now()
			if !task.Finished {
				c.SharedState.ReducePhaseDone = false

				if now.Sub(task.StartTime).Seconds() > TIMEOUT {
					c.SharedState.ReduceTasks[i].StartTime = now

					fileName = c.SharedState.ReduceTasks[i].FileName
					taskID = i
					break
				}
			}
		}

		if taskID != -1 {
			reply.TaskID = taskID
			reply.FileName = fileName
			reply.IsMapTask = false

			return nil
		}
	}

	// no task to assign
	reply.TaskID = -1

	if c.SharedState.MapPhaseDone && c.SharedState.ReducePhaseDone {
		reply.PleaseExit = true
		// log.Default().Printf("All tasks finished, please exit")
	}

	// log.Default().Printf("FindNextTaskHandler() -> taskID: %d", reply.TaskID)
	return nil
}

func (c *Coordinator) CheckForNewMapTask() (int, string) {
	for i, task := range c.SharedState.MapTasks {
		now := time.Now()
		if !task.Finished && now.Sub(task.StartTime).Seconds() > TIMEOUT {
			c.SharedState.MapTasks[i].StartTime = now

			return i, task.FileName
		}
	}

	return -1, ""
}

func (c *Coordinator) CheckForNewReduceTask() (int, string) {
	for i, task := range c.SharedState.ReduceTasks {
		now := time.Now()
		if !task.Finished && now.Sub(task.StartTime).Seconds() > TIMEOUT {
			c.SharedState.ReduceTasks[i].StartTime = now

			return i, task.FileName
		}
	}

	return -1, ""
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.SharedState.mu.Lock()
	defer c.SharedState.mu.Unlock()

	ret := false

	if c.SharedState.MapPhaseDone && c.SharedState.ReducePhaseDone {
		ret = true
	}

	return ret
}

func (c *Coordinator) initTaskLists(files []string) {
	for _, filename := range files {
		task := Task{filename, false, time.Time{}} // The zero value of type Time is January 1, year 1, 00:00:00.000000000 UTC
		c.SharedState.MapTasks = append(c.SharedState.MapTasks, task)
	}

	for i := 0; i < c.NReduce; i++ {
		fileName := fmt.Sprintf("mr-out-%d", i)
		task := Task{fileName, false, time.Time{}}
		c.SharedState.ReduceTasks = append(c.SharedState.ReduceTasks, task)
	}
}

// create a Coordinator.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	sharedState := &SharedState{MapTasks: []Task{}, ReduceTasks: []Task{}}

	c := Coordinator{NReduce: nReduce, SharedState: sharedState}
	c.initTaskLists(files)

	c.server()
	return &c
}
