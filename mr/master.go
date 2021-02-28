package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"


type Master struct {
	// Your definitions here.
    MapWorks map[string]*WorkRecord

	ReduceWorks  map[string]*WorkRecord
	ReducersNum int

	Assigment map[string][]*WorkRecord
	mapMutex sync.Mutex
    reduceMutex sync.Mutex
}

func (m *Master) getNextMapWork() *WorkRecord{
	m.mapMutex.Lock()
	defer m.mapMutex.Unlock()
	for _, v := range m.MapWorks {
		if !v.Assigned {
			v.Assigned = true
			return v
		}
	}
	return nil
}

func (m *Master) getNextReduceWork() *WorkRecord{
    m.reduceMutex.Lock()
    m.reduceMutex.Unlock()
	for _, v := range m.ReduceWorks {
		if !v.Assigned {
			v.Assigned = true
			return v
		}
	}
	return nil
}
// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) Assign (args *AssignmentRequest, reply *AssignmentReply) error{

	if len(m.MapWorks) == 0 || len(m.ReduceWorks) == 0 {
		reply.Status = 404
		return nil
	}
	if len(m.MapWorks) == 0 && args.WorkType == "map" {
		reply.Status = 404
		return nil
	}
	if len(m.ReduceWorks) == 0 && args.WorkType == "reduce" {
		reply.Status = 404
		return nil
	}

	if args.WorkType == "map" {
		w := m.getNextMapWork()
		w.StartTime = time.Now()
		m.Assigment[args.WorkerID] = append(m.Assigment[args.WorkerID], w)
		reply.WorkType = "map"
		reply.Status = 200
		reply.NReduce = m.ReducersNum
		reply.Record = *w
		return nil
	}


	if args.WorkType == "reduce" {
		w := m.getNextReduceWork()
		w.StartTime = time.Now()
		m.Assigment[args.WorkerID] = append(m.Assigment[args.WorkerID], w)
		reply.WorkType = "reduce"
		reply.Status = 200
		reply.NReduce = m.ReducersNum
		reply.Record = *w
		return nil
	}

	reply.Status = 400
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	//sockname := masterSock()
	//os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.ReducersNum = nReduce
	for _, file := range files {
		record := &WorkRecord{
			ID:        file,
			Filename:  file,
		}
		m.MapWorks[record.ID] = record
	}

	m.server()
	return &m
}
