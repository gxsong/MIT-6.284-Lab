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
//
type ExampleArgs struct {
	X int
}

//
type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
// TODO: add documentations

// GetTaskArgs ...
type GetTaskArgs struct {
}

// GetTaskReply ...
type GetTaskReply struct {
	workerType      string // can only be "MAP" or "REDUCE" or "EXIT"
	workerID        int    // assigned by master, to be passed in the req of UpdateMapTaskState
	inputFileNames  []string
	outputFileNames []string
	err             error
}

// UpdateTaskStateArgs ...
type UpdateTaskStateArgs struct {
	workerID  int
	workerErr error // error encountered by the worker when executing mapper task
}

// UpdateTaskStateReply ...
type UpdateTaskStateReply struct {
	err error
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
