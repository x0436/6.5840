package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type TaskCompletedState int
const (
	MapTaskCompleted = iota
	MapTaskFailed
	ReduceTaskCompleted
	ReduceTaskFailed
)

type TaskType int
const (
	MapTask = iota
	ReduceTask
	Exit
)

type Message2Coordinator struct {
	TaskID int
	TaskCompletedState TaskCompletedState
}

type Message2Worker struct {
	TaskID int
	TaskType TaskType
	TaskFile string
	NMap int
	NReduce int
}

// Add your RPC definitions here.
