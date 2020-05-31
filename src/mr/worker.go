package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
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

func readInput(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return "", err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return "", err
	}
	file.Close()
	return string(content), nil
}

func writeMapOutput(outputFileName string, kvs []KeyValue) error {
	file, _ := os.Create(outputFileName)
	enc := json.NewEncoder(file)
	for _, kv := range kvs {
		err := enc.Encode(kv)
		if err != nil {
			return err
		}
	}
	return nil
}

// perform map task, note that map task should only have one input file
func runMap(mapf func(string, string) []KeyValue, inputFileNames []string, outputFileNames []string) error {
	log.Println("Map stub run")
	filename := inputFileNames[0]
	content, err := readInput(inputFileNames[0])
	if err != nil {
		return err
	}

	kvs := mapf(filename, content)
	nOutputPartitions := len(outputFileNames)

	fileKVMap := make(map[string][]KeyValue)
	for _, kv := range kvs {
		hash := ihash(kv.Key) % nOutputPartitions
		filename := outputFileNames[hash]
		fileKVMap[filename] = append(fileKVMap[filename], kv)
	}

	for filename, kvs := range fileKVMap {
		err := writeMapOutput(filename, kvs)
		if err != nil {
			return err
		}
	}
	return nil
}

// TODO: implement me!
// stub for reduce work
func runReduce() bool {
	log.Println("reduce stub run")
	return true
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
		ok := call("Master.GetTask", &getTaskArgs, &getTaskReply)
		if !ok {
			log.Printf("Failed calling Master.GetTask.")
			continue
		}

		taskType, TaskID := getTaskReply.TaskType, getTaskReply.TaskID
		updateTaskStateArgs, updateTaskStateReply := UpdateTaskStateArgs{}, UpdateTaskStateReply{}
		updateTaskStateArgs.TaskType, updateTaskStateArgs.TaskID = taskType, TaskID

		switch taskType {
		case MAP, REDUCE:
			log.Printf("got %s task on with id %d with input %s, output %s", taskType, TaskID, getTaskReply.InputFileNames, getTaskReply.OutputFileNames)
			var err error
			if taskType == MAP {
				err = runMap(mapf, getTaskReply.InputFileNames, getTaskReply.OutputFileNames)
			} else if taskType == REDUCE {
				runReduce()
			}
			if err != nil {
				updateTaskStateArgs.WorkerErr = err
			}
			ok = call("Master.UpdateTaskState", &updateTaskStateArgs, &updateTaskStateReply)
			if !ok {
				log.Printf("Failed calling Master.UpdateTaskState.")
				break
			}
			log.Printf("updated %s task on with id %d.", taskType, TaskID)
		case EXIT:
			log.Printf("got %s task.", taskType)
			return
		default:
			log.Printf("bad task type: %s.", taskType)
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
