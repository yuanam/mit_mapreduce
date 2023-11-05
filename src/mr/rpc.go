package mr

import (
	"os"
	"strconv"
)

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}
type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type Workerstate int

const (
	free Workerstate = iota
	finished
)

type RequestArgs struct {
	WorkerState Workerstate // 0-free 1-busy(judge free or not) 2-finished
	Taskid      int
}

type JobType int

// define the Jobtype's kinds
const (
	Mapjob JobType = iota
	Reducejob
	Waitjob
	Completejob
)

type ResponseArgs struct {
	Taskid       int // unique id
	Filename     string
	Jobtype      JobType //Mapjob\Reducejob\Waitjob\Completejob no string
	Mapnumber    int
	Reducenumber int // how decide
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
