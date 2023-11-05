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

type Taskstate int

const (
	Nostart Taskstate = iota
	Running
	Done
)

type Coordinatorstate int

const (
	Mapstate Coordinatorstate = iota
	Reducestate
	Finish
)

type Coordinator struct {

	// Your definitions here.

	mutex           sync.Mutex
	Inputfiles      []string
	Currentstate    Coordinatorstate
	Mapstatus       map[int]Taskstate
	Reducestatus    map[int]Taskstate
	nFinishedmap    int
	nFinishedreduce int
	nMap            int
	nReduce         int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Finished_status(reply *ResponseArgs) {
	reply.Jobtype = Completejob
}

func (c *Coordinator) Finish(args *RequestArgs) {
	c.mutex.Lock()
	defer c.mutex.Unlock() // todo defer
	switch c.Currentstate {
	case Mapstate:
		fmt.Println("map task finished id:", args.Taskid)
		c.Mapstatus[args.Taskid] = Done
		c.nFinishedmap += 1
		if c.nFinishedmap == len(c.Mapstatus) {
			fmt.Println("coordinator state changed to reduce")
			c.Currentstate = Reducestate
		}
		break
	case Reducestate:
		fmt.Println("reduce task finished id:", args.Taskid)
		c.Reducestatus[args.Taskid] = Done
		c.nFinishedreduce += 1
		if c.nFinishedreduce == len(c.Reducestatus) {
			fmt.Println("all tasks have been finished")
			c.Currentstate = Finish
		}
		break
	}
}

func (c *Coordinator) AssignTask(args *RequestArgs, reply *ResponseArgs) error {
	c.mutex.Lock()
	Coordinatorstate := c.Currentstate
	c.mutex.Unlock()
	switch args.WorkerState {
	// waiting for request
	case free:
		switch Coordinatorstate {
		case Mapstate:
			c.Assignmap(reply)
			break
		case Reducestate:
			c.Assignreduce(reply)
			break
		case Finish:
			c.Finished_status(reply)
			break
		}
		break

	case finished:
		c.Finish(args)
		break
	}
	return nil
}

func (c *Coordinator) Assignmap(reply *ResponseArgs) {
	reply.Jobtype = Mapjob
	c.mutex.Lock()
	defer c.mutex.Unlock()
	assigned_task := false
	for i := 0; i < len(c.Mapstatus); i++ {
		if c.Mapstatus[i] == Nostart {
			c.Mapstatus[i] = Running
			reply.Taskid = i
			assigned_task = true
			break
		}
	}
	if !assigned_task && c.nFinishedmap != c.nMap {
		reply.Jobtype = Waitjob
		return
	}
	reply.Filename = c.Inputfiles[reply.Taskid]

	reply.Mapnumber = c.nMap
	reply.Reducenumber = c.nReduce

	// If the task has not been completed within 10 seconds, the worker is considered dead.
	// The task will be given to other workers.
	go func(taskid int) {
		time.Sleep(time.Duration(10) * time.Second)
		c.mutex.Lock()
		defer c.mutex.Unlock()
		if c.Mapstatus[taskid] == Running {
			c.Mapstatus[taskid] = Nostart
		}
	}(reply.Taskid)
}

func (c *Coordinator) Assignreduce(reply *ResponseArgs) {
	reply.Jobtype = Reducejob
	c.mutex.Lock()
	defer c.mutex.Unlock()
	assigned_task := false
	for i := 0; i < len(c.Reducestatus); i++ {
		if c.Reducestatus[i] == Nostart {
			c.Reducestatus[i] = Running
			reply.Taskid = i
			assigned_task = true
			break
		}
	}
	if !assigned_task && c.nFinishedreduce != c.nReduce {
		reply.Jobtype = Waitjob
		return
	}
	reply.Mapnumber = c.nMap
	reply.Reducenumber = c.nReduce

	go func(taskId int) {
		time.Sleep(time.Duration(10) * time.Second)
		c.mutex.Lock()
		defer c.mutex.Unlock()
		if c.Reducestatus[taskId] == Running {
			c.Reducestatus[taskId] = Nostart
		}
	}(reply.Taskid)
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
	// ret := false

	// Your code here.

	c.mutex.Lock()
	ret := c.Currentstate == Finish
	c.mutex.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Inputfiles = files
	c.Currentstate = Mapstate
	c.nMap = len(files)
	c.nReduce = nReduce
	c.nFinishedmap = 0
	c.nFinishedreduce = 0
	c.Mapstatus = make(map[int]Taskstate, c.nMap)
	for i := 0; i < c.nMap; i++ {
		c.Mapstatus[i] = Nostart
	}
	c.Reducestatus = make(map[int]Taskstate, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.Reducestatus[i] = Nostart
	}

	c.server()
	return &c
}
