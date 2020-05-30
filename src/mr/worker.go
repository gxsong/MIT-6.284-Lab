package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// TODO: implement me!
// stub for map work
func runMap() {
	log.Println("Map stub run")
}

// TODO: implement me!
// stub for reduce work
func runReduce() {
	log.Println("reduce stub run")
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		// get task from master
		getTaskArgs, getTaskReply := GetTaskArgs{}, GetTaskReply{}
		log.Println("Worker calling Master.GetTask")
		// TODO: add error handling
		ok := call("Master.GetTask", &getTaskArgs, &getTaskReply)
		if !ok {
			log.Fatalf("Failed calling Master.GetTask.")
			continue
		}

		taskType, TaskID := getTaskReply.TaskType, getTaskReply.TaskID
		if TaskID == 1 {
			continue
		}
		updateTaskStateArgs, updateTaskStateReply := UpdateTaskStateArgs{}, UpdateTaskStateReply{}
		updateTaskStateArgs.TaskType, updateTaskStateArgs.TaskID = taskType, TaskID

		switch taskType {
		case MAP:
			log.Printf("got %s task on with id %d with input %s", taskType, TaskID, getTaskReply.InputFileNames[0])
			runMap()
			ok = call("Master.UpdateTaskState", &updateTaskStateArgs, &updateTaskStateReply)
			if !ok {
				log.Fatalf("Failed calling Master.UpdateTaskState.")
				break
			}
			log.Printf("updated %s task on with id %d.", taskType, TaskID)
		case REDUCE:
			log.Printf("got %s task on with id %d.", taskType, TaskID)
			runReduce()
			ok = call("Master.UpdateTaskState", &updateTaskStateArgs, &updateTaskStateReply)
			if !ok {
				log.Fatalf("Failed calling Master.UpdateTaskState.")
				break
			}
			log.Printf("updated %s task on with id %d.", taskType, TaskID)
		case EXIT:
			log.Printf("got %s task.", taskType)
			return
		default:
			log.Fatalf("bad task type: %s.", taskType)
			return
		}
		time.Sleep(time.Second * 3)
	}

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
