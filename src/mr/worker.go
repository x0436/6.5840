package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
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
		report.TaskID = task.TaskID

		switch task.TaskType {
		case MapTask:
			HandleMapTask(&task, mapf)
			report.TaskCompletedState = MapTaskCompleted
			CallForReport(&report, &task)
		case ReduceTask:
			HandleReduceTask(&task, reducef)
			report.TaskCompletedState = ReduceTaskCompleted
			CallForReport(&report, &task)
		case WAIT:
			time.Sleep(1 * time.Second)
		default:
			time.Sleep(1 * time.Second)
		}
		// TODO: 退出循环呢
	}
}

func CallForTask(args *MessageReport, reply *MessageTask) {
	ok := call("Coordinator.GetTask", args, reply)
	if ok {
		fmt.Printf("Get Task succeed!\n")
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallForReport(args *MessageReport, reply *MessageTask) {
	ok := call("Coordinator.ReportTask", args, reply)
	if ok {
		fmt.Printf("Report Task succeed!\n")
	} else {
		fmt.Printf("call failed!\n")
	}
}

func HandleMapTask(task *MessageTask, mapf func(string, string) []KeyValue) {
	NReduce := task.NReduce
	filename := task.TaskFile
	intermediate := make([][]KeyValue, NReduce)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	// 遍历kv对切片, 将其放进对应的中间文件
	for _, kv := range kva {
		key := kv.Key
		idx := ihash(key) % NReduce
		intermediate[idx] = append(intermediate[idx], kv)
	}

	// 遍历每个中间文件 intermediate[i] 生成 mr-X-Y
	for i := 0; i < NReduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d", task.TaskID, i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)

		// sort.Sort(ByKey(intermediate[i]))

		for _, kv := range intermediate[i] {
			// fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
			enc.Encode(&kv)
		}
	}
}

func HandleReduceTask(task *MessageTask, reducef func(string, []string) string) {
	NMap := task.NMap
	intermediate := []KeyValue{}

	// 枚举每个中间文件, 获取kv对后合并到intermediate
	for i := 0; i < NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, task.TaskID)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	generateOutputFile(task.TaskID, intermediate, reducef)
}
func generateOutputFile(taskID int, intermediate []KeyValue, reducef func(string, []string) string) {
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", taskID)

	ofile, _ := os.Create(oname)
	fmt.Printf("Create file mr-out-%d\n", taskID)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
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
