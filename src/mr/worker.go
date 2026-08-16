package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	for {
		report := MessageReport{}
		task := MessageTask{}

		CallForTask(&report, &task)

		switch task.TaskType {
		case MapTask:
			HandleMapTask()
		case ReduceTask:
			HandleReduceTask()
		case WAIT:
			time.Sleep(1 * time.Second)
		}

		CallForReport(&report, &task)
	}
}

func CallForTask(args *MessageReport, reply *MessageTask) {
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		fmt.Printf("Get Task succeed!\n")
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallForReport(args *MessageReport, reply *MessageTask) {
	ok := call("Coordinator.ReportTask", &args, &reply)
	if ok {
		fmt.Printf("Report Task succeed!\n")
	} else {
		fmt.Printf("call failed!\n")
	}
}

func HandleMapTask() {

}

func HandleReduceTask() {

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
