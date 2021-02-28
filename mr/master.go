package mr

import (
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

const TIME_OUT time.Duration = 10 * time.Second

type Master struct {
	// Your definitions here.
	MapWorks         map[string]*WorkRecord
	ReduceWorks      map[string]*WorkRecord
	Assigment        map[string][]*WorkRecord
	availableWorkers map[string]time.Time

	ReducersNum int
	succeedReduce int32
	mapMutex    sync.Mutex
	reduceMutex sync.Mutex
	workerMutex sync.Mutex
	assignmentMutex sync.Mutex
	stopCheck   chan struct{}
}

func (m *Master) getNextMapWork() *WorkRecord {
	m.mapMutex.Lock()
	defer m.mapMutex.Unlock()
	for _, v := range m.MapWorks {
		if !v.getAssigned() {
			v.updateAssigned(true)
			return v
		}
	}
	return nil
}

func (m *Master) getNextReduceWork() *WorkRecord {
	m.reduceMutex.Lock()
	m.reduceMutex.Unlock()
	for _, v := range m.ReduceWorks {
		if !v.getAssigned() {
			v.updateAssigned(true)
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

func (m *Master) heartBeat(args *HeartbeatRequest, reply *HeartbeatResponse) error {

	workerID := args.WorkerID
	m.updateWorkLive(workerID)
	reply.Status = 200
	return nil
}

func (m *Master) updateWorkLive(workerID string) {
	m.workerMutex.Lock()
	defer m.workerMutex.Unlock()
	m.availableWorkers[workerID] = time.Now()
}

func (m *Master) finish(args *FinishRequest, reply *FinishResponse) error {

	workerID := args.WorkerID
	m.updateWorkLive(workerID)
	workID := args.ID
	if args.WorkType == "reduce" {
		m.reduceMutex.Lock()
		defer m.reduceMutex.Unlock()
		if m.ReduceWorks[workID].Finished {
			reply.Status = 400
			return nil
		}
		atomic.AddInt32(&m.succeedReduce, 1)
		m.ReduceWorks[workID].Finished = true
		reply.Status = 200
		return nil
	}

	if args.WorkType == "map" {
		m.mapMutex.Lock()
		if m.MapWorks[workID].Finished {
			reply.Status = 400
			return nil
		}
		m.mapMutex.Unlock()
		var locations []string
		copy(locations, args.locations)
		reduceWork := &WorkRecord{
			locations: locations,
			Assigned:  false,
			Finished:  false,
			StartTime: time.Time{},
		}
		m.reduceMutex.Lock()
		reduceWork.ID = strconv.Itoa(len(m.ReduceWorks))
		m.ReduceWorks[reduceWork.ID] = reduceWork
		m.reduceMutex.Unlock()
		reply.Status = 200
		return nil
	}
	reply.Status = 400
	return nil
}

func (m *Master) Assign(args *AssignmentRequest, reply *AssignmentReply) error {

	if len(m.MapWorks) == 0 || len(m.ReduceWorks) == 0 {
		reply.Status = 404
		return nil
	}
	m.updateWorkLive(args.WorkerID)
	w := m.getNextMapWork()
	if w != nil {
		w.StartTime = time.Now()
		m.assignmentMutex.Lock()
		w.WorkerID = args.WorkerID
		m.Assigment[args.WorkerID] = append(m.Assigment[args.WorkerID], w)
		m.assignmentMutex.Unlock()
		reply.WorkType = "map"
		reply.Status = 200
		reply.NReduce = m.ReducersNum
		reply.Record = WorkRecord{
			ID:        w.ID,
			WorkerID:  w.WorkerID,
			locations: w.locations,
			Assigned:  w.Assigned,
			Finished:  w.Finished,
			StartTime: w.StartTime,
		}
		return nil
	}

	w = m.getNextReduceWork()
	if w != nil {
		w.StartTime = time.Now()
		m.assignmentMutex.Lock()
		w.WorkerID = args.WorkerID
		m.Assigment[args.WorkerID] = append(m.Assigment[args.WorkerID], w)
		m.assignmentMutex.Unlock()
		reply.WorkType = "reduce"
		reply.Status = 200
		reply.NReduce = m.ReducersNum
		reply.Record = WorkRecord{
			ID:        w.ID,
			WorkerID:  w.WorkerID,
			locations: w.locations,
			Assigned:  w.Assigned,
			Finished:  w.Finished,
			StartTime: w.StartTime,
		}
		return nil
	}

	reply.Status = 400
	return nil
}

func (m *Master) checkWorkers() {

	ticker := time.Tick(100 * time.Microsecond)

	for {

		select {
		    case <- ticker:
				m.workerMutex.Lock()
				defer m.workerMutex.Unlock()
				for worker, updateTime := range m.availableWorkers {

					if time.Now().Sub(updateTime) > TIME_OUT {
						delete(m.availableWorkers, worker)
						for _, w:= range m.Assigment[worker] {
							w.updateAssigned(false)
						}
					}
				}
			case <- m.stopCheck:
				return
		}
	}
}

func (m *Master) checkWorks() {

	ticker := time.Tick(100 * time.Microsecond)

	for {
		select {
		  case <- ticker:
			  m.mapMutex.Lock()
			  for _, w := range m.MapWorks {
				  if time.Now().Sub(w.StartTime) > TIME_OUT {
					  w.updateAssigned(false)
				  }
			  }
			  m.mapMutex.Unlock()

			  m.reduceMutex.Lock()
			  for _, w := range m.ReduceWorks {
				  if time.Now().Sub(w.StartTime) > TIME_OUT {
					  w.updateAssigned(false)
				  }
			  }
			  m.reduceMutex.Unlock()
		case <- m.stopCheck:
			return
		}
	}


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
	m.stopCheck = make(chan struct{})
	m.MapWorks = make(map[string]*WorkRecord)
	m.ReduceWorks = make(map[string]*WorkRecord)
	m.availableWorkers = make(map[string]time.Time)
	m.Assigment = make(map[string][]*WorkRecord)

	for _, file := range files {
		record := &WorkRecord{
			ID:        file,
			locations: []string{file},
		}
		m.MapWorks[record.ID] = record
	}

	go m.checkWorkers()
	go m.checkWorks()
	m.server()
	return &m
}
