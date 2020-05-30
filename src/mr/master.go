package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

const numMapTask = 8

// MasterIf - an interface that contains master api
type MasterIf interface {
	GetTask(args *GetTaskArgs, reply *GetTaskReply)
	UpdateTaskState(args *UpdateTaskStateArgs, reply *UpdateTaskStateReply)
}

type taskInfo struct {
	workerID        int
	done            bool
	inputFileNames  []string
	outputFileNames []string
}

type taskMap struct {
	mutex     sync.Mutex
	taskState map[int]taskInfo
}

type fileMap struct {
	mutex     sync.Mutex
	fileState map[string]bool
}

// Master - implements MasterIf, MapReduce master
type Master struct {
	numMapTask         int
	numReduceTask      int
	originalInputFiles fileMap // stores original input files, marks if it's already assigned to a map worker
	intermediateFiles  fileMap // stores intermediate file names, marks if it's already assigned to a reduce worker
	outputFiles        fileMap // stores output file names, marks if it's successfully written by a reduce worker
	mapTasks           taskMap
	reduceTasks        taskMap
	allMapDone         bool // true if all map tasks done -- ready to begin reduce
	allReduceDone      bool // true if all reduce tasks done -- ready to exit
}

// Your code here -- RPC handlers for the worker to call.

// GetTask ...
func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	log.Println("Master.GetTask called")
	reply.TaskType = EXIT
	return nil
}

// UpdateTaskState ...
func (m *Master) UpdateTaskState(args *UpdateTaskStateArgs, reply *UpdateTaskStateReply) error {
	// TODO: implement handler
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	fmt.Println("got call from client")
	m.allReduceDone = true
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
// check if the job is done
// return true if the job has finished, false otherwise
//
func (m *Master) Done() bool {
	return m.allMapDone && m.allReduceDone
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// TODO: prepare input files as tasks to be assigned to mappers
	// NOTE: # of mapper tasks = # of split files. suppose the pg-xxx.txt files are already split

	m.server()
	return &m
}
