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

// TaskType ...
type TaskType int

// documentation Place Holder
const (
	MAP TaskType = iota
	REDUCE
	EXIT
)

func (t TaskType) String() string {
	return []string{"MAP", "REDUCE", "EXIT"}[t]
}

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
	TaskType        TaskType
	TaskID          int // assigned by master, to be passed in the req of UpdateMapTaskState
	InputFileNames  []string
	OutputFileNames []string
}

// UpdateTaskStateArgs ...
type UpdateTaskStateArgs struct {
	TaskType  TaskType
	TaskID    int
	WorkerErr error // error encountered by the worker when executing mapper task
}

// UpdateTaskStateReply ...
type UpdateTaskStateReply struct {
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
