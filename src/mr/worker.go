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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func getBucketIndex(key string, cnt int) int {
	return ihash(key) % cnt
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	CallForTask(mapf, reducef)
}

func CallForTask(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}

		ok := call("Coordinator.FindNextTaskHandler", &args, &reply)
		if ok {
			if reply.PleaseExit {
				// shut down here
				// log.Default().Printf("CallForTask() -> please exit")
				os.Exit(0)
			}

			if reply.TaskID == -1 {
				// wait for getting the mutex
				time.Sleep(3 * time.Second)
				// log.Default().Printf("CallForTask() -> taskID is -1, continue to wait")
				continue
			}

			if reply.IsMapTask {
				_, err := DoMapTask(reply, mapf)
				if err == nil {
					CallTaskFinished(reply.TaskID, true)
				} else {
					log.Default().Printf("CallForTask() -> DoMapTask err: %s", err.Error())
				}
			} else {
				err := DoReduceTask(reply, reducef)
				if err == nil {
					CallTaskFinished(reply.TaskID, false)
				} else {
					log.Default().Printf("CallForTask() -> DoReduceTask err: %s", err.Error())
				}
			}
		} else {
			fmt.Printf("call failed!\n")
		}
	}
}

func DoMapTask(reply GetTaskReply, mapf func(string, string) []KeyValue) ([]KeyValue, error) {
	// log.Default().Printf("DoMapTask() -> with id: %d, fileName: %s", reply.TaskID, reply.FileName)

	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("DoMapTask() -> cannot open %v", reply.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("DoMapTask() -> cannot read %v", reply.FileName)
	}
	file.Close()

	kva := mapf(reply.FileName, string(content))

	buckets := make([][]KeyValue, reply.NReduce)
	length := len(kva)
	i := 0
	for i < length {
		currentKey := kva[i].Key
		currentBucketIndex := getBucketIndex(currentKey, reply.NReduce)
		j := i
		for j < length && kva[j].Key == currentKey {
			j++
		}
		for k := i; k < j; k++ {
			buckets[currentBucketIndex] = append(buckets[currentBucketIndex], kva[k])
		}

		i = j
	}

	for reduceTaskID, pairs := range buckets {
		file, err := ioutil.TempFile(".", "map-tmp")
		if err != nil {
			fmt.Print("error creating tmp file")
		}

		enc := json.NewEncoder(file)
		for _, kv := range pairs {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode %v: %s", kv, err.Error())
			}
		}

		err = os.Rename(file.Name(), fmt.Sprintf("mr-%d-%d", reply.TaskID, reduceTaskID))
		if err != nil {
			fmt.Print("error renaming map tmp file")
		}
	}

	return kva, err
}

func DoReduceTask(reply GetTaskReply, reducef func(string, []string) string) error {
	// log.Default().Printf("DoReduceTask() -> with id: %d, fileName: %s", reply.TaskID, reply.FileName)

	kva := []KeyValue{}

	// read local intermediate files
	for mapTaskID := 0; mapTaskID < reply.NMap; mapTaskID++ {
		fileName := fmt.Sprintf("mr-%d-%d", mapTaskID, reply.TaskID)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("DoReduceTask() -> cannot open %v", fileName)
			// continue
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	// Sort intermediate keys
	sort.Sort(ByKey(kva))

	outputFile, err := ioutil.TempFile(".", "reduce-tmp")
	if err != nil {
		fmt.Print("error")
	}

	// get file in bucket
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)
		fmt.Fprintf(outputFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	err = os.Rename(outputFile.Name(), fmt.Sprintf("mr-out-%d", reply.TaskID))

	return err
}

func CallTaskFinished(taskID int, isMapTask bool) {
	args := FinishTaskArgs{taskID, isMapTask}
	reply := FinishTaskArgs{}

	ok := call("Coordinator.TaskFinishedHandler", &args, &reply)
	if !ok {
		// fmt.Printf("reply.TaskID %v\n", reply.TaskID)
		fmt.Printf("call failed!\n")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
