package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
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

type WorkRecord struct {
	ID      string
	Filename string
	Assigned bool
	Finished bool
	StartTime time.Time
}

type AssignmentRequest struct {
	WorkerID  string
	WorkType  string
}

type AssignmentReply struct {
	Status    int
	WorkType  string
	NReduce   int
	Record WorkRecord
}

type FinishRequest struct {
	ID  string
	WorkerID string
	WorkType string
	locations []string
}

type FinishResponse struct {
	Status  int
}

type HeartbeatRequest struct {
	WorkerID  string
}

type HeartbeatResponse struct {
	Response  string
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