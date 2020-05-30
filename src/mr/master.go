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

const numMapTask = 3

type taskState int

// placeholder
const (
	CREATED taskState = iota
	ASSIGNED
	UPDATED
)

func (t taskState) String() string {
	return []string{"CREATED", "ASSIGNED", "UPDATED"}[t]
}

// MasterIf - an interface that contains master api
type MasterIf interface {
	GetTask(args *GetTaskArgs, reply *GetTaskReply)
	UpdateTaskState(args *UpdateTaskStateArgs, reply *UpdateTaskStateReply)
}

type task struct {
	taskID          int
	state           taskState
	inputFileNames  []string
	outputFileNames []string
}

type taskMap struct {
	mutex sync.Mutex
	tasks map[int]*task // taskID -> task
}

// Master - implements MasterIf, MapReduce master
type Master struct {
	numMapTask         int
	numReduceTask      int
	originalInputFiles []string
	intermediateFiles  []string
	outputFiles        []string
	mapTasksToDo       taskMap
	reduceTasksToDo    taskMap
}

// helper functions goes here

func (m *Master) isDone(taskType TaskType) bool {
	var tasks map[int]*task
	switch taskType {
	case MAP:
		m.mapTasksToDo.mutex.Lock()
		defer m.mapTasksToDo.mutex.Unlock()
		tasks = m.mapTasksToDo.tasks
		for _, t := range tasks {
			if t.state != UPDATED {
				log.Printf("checking if mapping is done: found taskID %d is not: %s", t.taskID, t.state)
				return false
			}
		}
		return true
	case REDUCE:
		// TODO: similar as above
		return false
	default:
		log.Fatalf("bad task type: %s.", taskType)
		return false
	}
}

func (m *Master) waitAndSetTaskDone(taskID int, taskType TaskType) error {
	// TODO: wait for 10 secs to see if tasks is done,
	// if yes, set task to done, else return error
	// NOTE: don't lock before waiting for 10 sec
	time.Sleep(time.Second * 10)
	switch taskType {
	case MAP:
		m.mapTasksToDo.mutex.Lock()
		log.Printf("checking taskID %d is done: %s", taskID, m.mapTasksToDo.tasks[taskID].state)
		if m.mapTasksToDo.tasks[taskID].state != UPDATED {
			m.mapTasksToDo.tasks[taskID].state = CREATED
		}
		m.mapTasksToDo.mutex.Unlock()
	case REDUCE:
		m.reduceTasksToDo.mutex.Lock()
		log.Printf("dummy lock on task map")
		m.reduceTasksToDo.mutex.Unlock()
	}

	return nil
}

// Your code here -- RPC handlers for the worker to call.

// GetTask ...
func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {

	// TODO: assign task with reply fields. only assign reduce task if all map tasks finished.

	if !m.isDone(MAP) {
		// assign map task
		log.Println("Master.GetTask called")
		m.mapTasksToDo.mutex.Lock()
		// find a task that hasn't been done
		var taskID int
		var t *task
		for taskID, t = range m.mapTasksToDo.tasks {
			if t.state == CREATED {
				break
			}
		}
		reply.TaskID = taskID
		reply.TaskType = MAP
		reply.InputFileNames = t.inputFileNames
		log.Printf("Assigning %s task %d, state is %s.", reply.TaskType, reply.TaskID, t.state)
		t.state = ASSIGNED
		m.mapTasksToDo.mutex.Unlock()
		go m.waitAndSetTaskDone(taskID, reply.TaskType)

	} else if false {
		// TODO: add REDUCE case
		// assign reduce task
	} else {
		reply.TaskID = -10000
		reply.TaskType = EXIT
	}

	return nil
}

// UpdateTaskState ...
func (m *Master) UpdateTaskState(args *UpdateTaskStateArgs, reply *UpdateTaskStateReply) error {
	// TODO: implement handler
	log.Println("Master.UpdateTaskState called")
	taskType := args.TaskType
	taskID := args.TaskID
	switch taskType {
	case MAP:
		log.Printf("Updating %s task %d.", taskType, taskID)
		m.mapTasksToDo.mutex.Lock()
		// do not update if state == CREATED,
		// which means worker has timed out and its state
		// has been changed from ASSIGNED to CREATED
		if m.mapTasksToDo.tasks[taskID].state == ASSIGNED {
			m.mapTasksToDo.tasks[taskID].state = UPDATED
		}
		log.Printf("updated taskID %d is done: %s", taskID, m.mapTasksToDo.tasks[taskID].state)
		m.mapTasksToDo.mutex.Unlock()
	case REDUCE:
		log.Printf("Updating %s task %d.", taskType, taskID)
	default:
		log.Fatalf("bad task type: %s.", taskType)
	}
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
	return false
	// return m.isDone(MAP) && m.isDone(REDUCE)
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.numMapTask = numMapTask
	m.numReduceTask = nReduce
	// TODO: prepare input files as tasks to be assigned to mappers, populate m.mapTasksToDo
	// NOTE: # of mapper tasks = # of split files. suppose the pg-xxx.txt files are already split
	m.mapTasksToDo.tasks = make(map[int]*task)
	for i := 0; i < m.numMapTask; i++ {
		m.mapTasksToDo.tasks[i] = &task{i, CREATED, []string{"sample in"}, []string{"sample out"}}
	}
	// TODO: use a for loop to wait for all map done, then prepare reduce tasks, similar to mapper tasks as above
	m.server()
	return &m
}
