package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func readMapInput(inputFileName string) (string, error) {
	file, err := os.Open(inputFileName)
	if err != nil {
		return "", err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
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
	log.Println("Map worker is working")
	inputFileName := inputFileNames[0]
	content, err := readMapInput(inputFileNames[0])
	if err != nil {
		return err
	}

	kvs := mapf(inputFileName, content)
	nOutputPartitions := len(outputFileNames)

	fileKVMap := make(map[string][]KeyValue)
	for _, kv := range kvs {
		hash := ihash(kv.Key) % nOutputPartitions
		outputFileName := outputFileNames[hash]
		fileKVMap[outputFileName] = append(fileKVMap[outputFileName], kv)
	}

	for outputFileName, kvs := range fileKVMap {
		err := writeMapOutput(outputFileName, kvs)
		if err != nil {
			return err
		}
	}
	return nil
}

func readReduceInput(inputFileName string) ([]KeyValue, error) {
	kvs := []KeyValue{}
	file, _ := os.Open(inputFileName)
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			if err.Error() != "EOF" {
				return kvs, err
			}
			return kvs, nil
		}
		kvs = append(kvs, kv)

	}
	return kvs, nil
}

func writeReduceOutput(outputFileName string, content string) error {
	file, _ := os.Create(outputFileName)
	_, err := fmt.Fprintf(file, content)
	if err != nil {
		return err
	}
	file.Close()
	return nil
}

// perform reduce task, note that there is only one output file
func runReduce(reducef func(string, []string) string, inputFileNames []string, outputFileNames []string) error {
	log.Println("Reduce worker is working")
	allKVs := []KeyValue{}
	for _, inputFileName := range inputFileNames {
		kvs, err := readReduceInput(inputFileName)
		if err != nil {
			return err
		}
		allKVs = append(allKVs, kvs...)
	}
	sort.Sort(ByKey(allKVs))
	content := ""
	i := 0
	for i < len(allKVs) {
		j := i + 1
		for j < len(allKVs) && allKVs[j].Key == allKVs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, allKVs[k].Value)
		}
		res := reducef(allKVs[i].Key, values)
		content += fmt.Sprintf("%v %v\n", allKVs[i].Key, res)

		i = j
	}
	writeReduceOutput(outputFileNames[0], content)
	return nil
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

		taskType, TaskID, inputFileNames, outputFileNames := getTaskReply.TaskType, getTaskReply.TaskID, getTaskReply.InputFileNames, getTaskReply.OutputFileNames
		updateTaskStateArgs, updateTaskStateReply := UpdateTaskStateArgs{}, UpdateTaskStateReply{}
		updateTaskStateArgs.TaskType, updateTaskStateArgs.TaskID = taskType, TaskID

		switch taskType {
		case MAP, REDUCE:
			log.Printf("got %s task on with id %d, input %s, output %s", taskType, TaskID, getTaskReply.InputFileNames, getTaskReply.OutputFileNames)
			var err error
			if taskType == MAP {
				err = runMap(mapf, inputFileNames, outputFileNames)
			} else if taskType == REDUCE {
				err = runReduce(reducef, inputFileNames, outputFileNames)
			}
			if err != nil {
				updateTaskStateArgs.WorkerErr = err
			}
			ok = call("Master.UpdateTaskState", &updateTaskStateArgs, &updateTaskStateReply)
			if !ok {
				log.Printf("Failed calling Master.UpdateTaskState.")
				break
			}
			log.Printf("updated %s task with id %d.", taskType, TaskID)
		case EXIT:
			log.Printf("got %s task.", taskType)
			return
		default:
			log.Printf("bad task type: %s.", taskType)
			return
		}
		time.Sleep(time.Second)
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
