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

const (
	MapfType = iota // 0
	ReducefType
	WaitingType
	WorkerFinishedType
)

const (
	TaskStateIdle = iota
	TaskStateInprogress
	TaskStateDone
)

type Coordinator struct {
	// Your definitions here.
	IdleTasks            chan int
	InprogressTasks      chan int
	DoneTasks            chan int
	TasksTimer           map[int]int
	TasksState           map[int]int
	currentMapTaskNum    int
	currentReduceTaskNum int
	nReduce              int
	nMap                 int
	mutex                sync.Mutex
	filenames            []string
	TaskId               int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) TaskCall(args *TaskArgs, reply *TaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	//worker is waiting for task , give task
	if args.Type == WaitingType && !args.Finshed {

		//give task
		if c.currentMapTaskNum > 0 && c.TaskId < c.nMap {

			reply.TaskId = c.TaskId
			reply.Type = MapfType
			reply.Filename = c.filenames[c.TaskId]
			c.TasksTimer[c.TaskId] = 10
			c.TasksState[c.TaskId] = TaskStateInprogress

			select {
			case task := <-c.IdleTasks:
				c.InprogressTasks <- task
			default:
				// 没有可用任务，不做操作
			}

			c.TaskId++

		} else if c.currentReduceTaskNum > 0 && c.currentMapTaskNum == 0 && c.TaskId < c.nReduce {
			print(c.TaskId)
			print(" ")
			reply.TaskId = c.TaskId
			reply.Type = ReducefType
			c.TasksState[reply.TaskId] = TaskStateInprogress
			c.TasksTimer[reply.TaskId] = 10

			select {
			case task := <-c.InprogressTasks:
				c.DoneTasks <- task
			default:
				// 没有可用任务，不做操作
			}
			c.TaskId++
		}

	}

	if args.Finshed {
		c.TasksState[args.TaskId] = TaskStateDone
		select {
		case task := <-c.InprogressTasks:
			c.DoneTasks <- task
		default:
			// 没有可用任务，不做操作
		}

		switch args.Type {
		case MapfType:
			c.currentMapTaskNum--
		case ReducefType:
			c.currentReduceTaskNum--
		}

		//Map tasks are done , and and reduce not start, start reduce tasks
		if c.currentMapTaskNum == 0 && c.currentReduceTaskNum == c.nReduce {
			c.TaskId = 0
			for i := 0; i < c.nReduce; i++ {
				c.InprogressTasks <- i
				c.TasksState[i] = TaskStateIdle
				c.TasksTimer[i] = -1
			}
		}

		if c.currentMapTaskNum == 0 && c.currentReduceTaskNum == 0 {
			reply.Exit = true
		}
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

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
	ret := false

	// Your code here.
	if c.currentMapTaskNum == 0 && c.currentReduceTaskNum == 0 {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{}
	c.nReduce = nReduce
	c.nMap = len(files)
	c.currentReduceTaskNum = nReduce
	c.currentMapTaskNum = len(files)
	c.TasksTimer = make(map[int]int, len(files))
	c.TasksState = make(map[int]int, nReduce)
	c.filenames = files

	c.TaskId = 0
	c.IdleTasks = make(chan int, len(files)+nReduce)
	c.InprogressTasks = make(chan int, len(files)+nReduce)
	c.DoneTasks = make(chan int, len(files)+nReduce)
	// Your code here.
	for i := 0; i < len(files); i++ {
		c.TasksState[i] = TaskStateIdle
		c.TasksTimer[i] = -1
	}

	//workers are call by worker.go in cmd, not in here

	c.server()
	go c.Timer()
	return &c
}

func (c *Coordinator) Timer() {
	for {
		c.mutex.Lock()

		for i := 0; i < len(c.TasksTimer); i++ {
			if c.TasksTimer[i] > 0 {
				c.TasksTimer[i]--
			}
			if c.TasksTimer[i] == 0 && c.TasksState[i] != TaskStateDone {
				c.TasksState[i] = TaskStateIdle
				select {
				case task := <-c.InprogressTasks:
					c.IdleTasks <- task
				default:
					// 没有可用任务，不做操作
				}
				c.TasksTimer[i] = -1
			}
		}
		c.mutex.Unlock()

		if c.Done() {
			break
		}

		time.Sleep(time.Second)
	}
}
