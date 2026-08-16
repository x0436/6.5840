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
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	reply.NMap = c.NMap
	reply.NReduce = c.NReduce
	if !c.AllMapTaskCompleted {
		reply.TaskType = MapTask
		return c.assignTask(c.MapTasks, reply)
	}
	if !c.AllReduceTaskCompleted {
		reply.TaskType = ReduceTask
		return c.assignTask(c.ReduceTasks, reply)
	}
	reply.TaskType = Exit
	return nil
}
func (c *Coordinator) assignTask(tasks []TaskInfo, reply *MessageTask) error {
	// 优先分配未分配任务
	for i := range tasks {
		if tasks[i].TaskState == Unassigned {
			return c.assign(tasks, i, reply)
		}
	}

	// 再分配失败或超时任务
	for i := range tasks {
		if tasks[i].TaskState == Failed ||
			(tasks[i].TaskState == Assigned &&
				time.Since(tasks[i].TimeStamp) > 10*time.Second) {
			return c.assign(tasks, i, reply)
		}
	}

	// 当前没有可分配的任务
	reply.TaskType = WAIT
	return nil
}
func (c *Coordinator) assign(tasks []TaskInfo, id int, reply *MessageTask) error {
	task := &tasks[id]

	task.TaskState = Assigned
	task.TimeStamp = time.Now()

	reply.TaskID = id
	reply.TaskFile = task.TaskFile

	return nil
}

func (c *Coordinator) ReportTask(args *MessageReport, reply *MessageTask) error {
	// 不使用第二个参数
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if args.TaskCompletedState == MapTaskCompleted {
		c.MapTasks[args.TaskID].TaskState = Completed
		return nil
	} else if args.TaskCompletedState == MapTaskFailed {
		c.MapTasks[args.TaskID].TaskState = Failed
		return nil
	} else if args.TaskCompletedState == ReduceTaskCompleted {
		c.ReduceTasks[args.TaskID].TaskState = Completed
		return nil
	} else if args.TaskCompletedState == ReduceTaskFailed {
		c.ReduceTasks[args.TaskID].TaskState = Failed
		return nil
	}
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
	return c.AllMapTaskCompleted && c.AllReduceTaskCompleted
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NMap:                   len(files),
		NReduce:                nReduce,
		MapTasks:               make([]TaskInfo, len(files)),
		ReduceTasks:            make([]TaskInfo, nReduce),
		AllMapTaskCompleted:    false,
		AllReduceTaskCompleted: false,
		Mutex:                  sync.Mutex{},
	}

	c.InitMapTask(files)
	c.InitReduceTask()

	c.server(sockname)
	return &c
}

func (c *Coordinator) InitMapTask(files []string) {
	// 遍历files 获得 那啥
	for i, file := range files {
		c.MapTasks[i] = TaskInfo{
			TaskState: Unassigned,
			TaskFile:  file,
			TimeStamp: time.Now(),
		}
	}
}
func (c *Coordinator) InitReduceTask() {
	for i := range c.ReduceTasks {
		c.ReduceTasks[i] = TaskInfo{
			TaskState: Unassigned,
			TimeStamp: time.Now(),
		}
	}
}
