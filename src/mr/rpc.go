package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

// Add your RPC definitions here
type RequestTaskArgs struct {
}

type RequestTaskReply struct {
	IsMap    bool
	Filename string
	Done     bool
	TaskID   int
	Nreduce  int
	Nfiles   int
}

type TaskDoneArgs struct {
	IsMap  bool
	TaskID int
}

type TaskDoneReply struct {
	Done bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
