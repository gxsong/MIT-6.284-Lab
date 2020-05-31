package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

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
	tasksToDo          map[TaskType]*taskMap
}

// helper functions goes here

func (m *Master) isDone(taskType TaskType) bool {
	if taskType != MAP && taskType != REDUCE {
		log.Printf("bad task type: %s.", taskType)
		return false
	}
	tMap := m.tasksToDo[taskType]
	tMap.mutex.Lock()
	defer tMap.mutex.Unlock()
	tasks := tMap.tasks
	for _, t := range tasks {
		if t.state != UPDATED {
			return false
		}
	}
	return true
}

func (m *Master) waitAndSetTaskDone(taskID int, taskType TaskType) error {
	if taskType != MAP && taskType != REDUCE {
		return errors.New("bad task type: " + string(taskType))
	}
	// NOTE: don't lock before waiting for 10 sec
	time.Sleep(time.Second * 10)

	tMap := m.tasksToDo[taskType]
	tMap.mutex.Lock()
	defer tMap.mutex.Unlock()
	if tMap.tasks[taskID].state != UPDATED {
		tMap.tasks[taskID].state = CREATED
		log.Printf("%s task %d timed out, will be reassigned to others", taskType, taskID)
	}
	return nil
}

func (m *Master) findToDoTask(tMap *taskMap) (taskID int, t *task) {
	tMap.mutex.Lock()
	defer tMap.mutex.Unlock()
	for taskID, t = range tMap.tasks {
		if t.state == CREATED {
			return taskID, t
		}
	}
	return -1, nil
}

func (m *Master) assignTask(taskType TaskType, reply *GetTaskReply) {
	tMap := m.tasksToDo[taskType]
	// find a task that hasn't been done
	var taskID int
	var t *task
	for {
		taskID, t = m.findToDoTask(tMap)
		if taskID != -1 && t != nil {
			break
		}
	}
	tMap.mutex.Lock()
	// populate response
	reply.TaskID = taskID
	reply.TaskType = taskType
	reply.InputFileNames = t.inputFileNames
	reply.OutputFileNames = t.outputFileNames
	log.Printf("Assigning %s task %d, state is %s.", reply.TaskType, reply.TaskID, t.state)

	t.state = ASSIGNED
	tMap.mutex.Unlock()
	go m.waitAndSetTaskDone(taskID, reply.TaskType)
}

// Your code here -- RPC handlers for the worker to call.

// GetTask ...
func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	if !m.isDone(MAP) {
		// assign map task
		m.assignTask(MAP, reply)
	} else if !m.isDone(REDUCE) {
		m.assignTask(REDUCE, reply)
	} else {
		reply.TaskID = -10000
		reply.TaskType = EXIT
	}

	return nil
}

// UpdateTaskState ...
func (m *Master) UpdateTaskState(args *UpdateTaskStateArgs, reply *UpdateTaskStateReply) error {
	taskType := args.TaskType
	taskID := args.TaskID
	var err error
	if taskType != MAP && taskType != REDUCE {
		log.Printf("bad task type: %s.", taskType)
		err = errors.New("bad task type")
	}
	tMap := m.tasksToDo[taskType]
	tMap.mutex.Lock()
	// do not update if state == CREATED,
	// which means worker has timed out and its state
	// has been changed from ASSIGNED to CREATED
	t := tMap.tasks[taskID]
	if args.WorkerErr != nil {
		t.state = CREATED
		log.Printf("Got worker err on %s task %d, will be reassigned.", taskType, taskID)
		err = errors.New("Worker Error")
	} else if t.state == ASSIGNED {
		t.state = UPDATED
		if taskType == MAP {
			m.intermediateFiles = append(m.intermediateFiles, t.outputFileNames...)
		} else if taskType == REDUCE {
			m.outputFiles = append(m.outputFiles, t.outputFileNames...)
		}
	}
	tMap.mutex.Unlock()
	log.Printf("updated taskID %d, state: %s", taskID, tMap.tasks[taskID].state)
	return err
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
	return m.isDone(MAP) && m.isDone(REDUCE)
}

func (m *Master) getMapOutputFiles(taskID int) []string {
	res := make([]string, m.numReduceTask)
	for i := 0; i < m.numReduceTask; i++ {
		name := fmt.Sprintf("mr-%d-%d", taskID, i)
		res[i] = name
	}
	return res
}

func (m *Master) getMapInputFiles(taskID int) []string {
	return []string{m.originalInputFiles[taskID]}
}

func (m *Master) getReduceInputFiles(taskID int) []string {
	res := make([]string, m.numMapTask)
	for i := 0; i < m.numMapTask; i++ {
		name := fmt.Sprintf("mr-%d-%d", i, taskID)
		res[i] = name
	}
	return res
}

func (m *Master) getReduceOutputFiles(taskID int) []string {
	return []string{fmt.Sprintf("mr-out-%d", taskID)}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.numMapTask = len(files)
	m.numReduceTask = nReduce
	m.originalInputFiles = files
	m.tasksToDo = map[TaskType]*taskMap{MAP: new(taskMap), REDUCE: new(taskMap)}

	// NOTE: # of mapper tasks = # of split files. suppose the pg-xxx.txt files are already split
	mapTasks := make(map[int]*task)
	for i := 0; i < m.numMapTask; i++ {
		mapTasks[i] = &task{i, CREATED, m.getMapInputFiles(i), m.getMapOutputFiles(i)}
	}
	m.tasksToDo[MAP].tasks = mapTasks

	reduceTasks := make(map[int]*task)
	for i := 0; i < m.numReduceTask; i++ {
		reduceTasks[i] = &task{i, CREATED, m.getReduceInputFiles(i), m.getReduceOutputFiles(i)}
	}
	m.tasksToDo[REDUCE].tasks = reduceTasks

	m.server()
	return &m
}
