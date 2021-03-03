package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"sync"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type Task struct {
	ID        string
	WorkerID  string
	Type      string
	Partition int
	Locations []string
	Assigned  bool
	Finished  bool
	StartTime time.Time
	m         sync.RWMutex
}

func (w *Task) updateAssigned(assigned bool) {
	w.m.Lock()
	defer w.m.Unlock()
	w.Assigned = assigned
}

func (w *Task) getAssigned() bool {
	w.m.RLock()
	defer w.m.RUnlock()
	return w.Assigned
}

type AssignmentRequest struct {
	WorkerID string
}

type AssignmentReply struct {
	Status  int
	NReduce int
	Record  Task
}

type FinishRequest struct {
	ID        string
	WorkerID  string
	WorkType  string
	Locations []string
}

type FinishResponse struct {
	Status int
}

type HeartbeatRequest struct {
	WorkerID string
}

type HeartbeatResponse struct {
	Status int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
