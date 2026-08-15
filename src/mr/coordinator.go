package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"

type TaskState int
const (
	Unassigned = iota
	Assigned
	Completed
	Failed
)

type TaskInfo struct {
	TaskState TaskState
	TaskFile  string
	TimeStamp time.Time
}

type Coordinator struct {
	NMap                   int
	NReduce                int
	MapTasks               []TaskInfo
	ReduceTasks            []TaskInfo
	AllMapTaskCompleted    bool
	AllReduceTaskCompleted bool
	Mutex                  sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *MessageReport, reply *MessageTask) error {
	// 不使用第一个参数
	return nil
}

func (c *Coordinator) ReportTask(args *MessageReport, reply *MessageTask) error {
	// 不使用第二个参数
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NMap: len(files),
		NReduce: nReduce,
		MapTasks: make([]TaskInfo, len(files)),
		ReduceTasks: make([]TaskInfo, nReduce),
		AllMapTaskCompleted: false,
		AllReduceTaskCompleted: false,
		Mutex: sync.Mutex{},
	}

	c.InitMapTask(files)
	c.InitReduceTask()

	c.server(sockname)
	return &c
}

func (c *Coordinator) InitMapTask(files []string) {

}
func (c *Coordinator) InitReduceTask() {

}
