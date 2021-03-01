package mr

import (
	"fmt"
	"log"
	"strconv"
	"strings"
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
	Tasks map[string]*WorkRecord

	Assigment        map[string][]*WorkRecord
	availableWorkers map[string]time.Time
	ReduceFileLocations map[int][]string

	ReducersNum     int
	MapNum          int
	succeedMap      int32
	succeedReduce   int32
	taskMutex       sync.Mutex
	workerMutex     sync.Mutex
	assignmentMutex sync.Mutex
	locationMutex   sync.Mutex
	stopCheck       chan struct{}
}

func (m *Master) getNextTask() *WorkRecord {
	m.taskMutex.Lock()
	defer m.taskMutex.Unlock()
	for _, v := range m.Tasks {
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

func (m *Master) HeartBeat(args *HeartbeatRequest, reply *HeartbeatResponse) error {
	workerID := args.WorkerID
	m.updateWorkerLive(workerID)
	reply.Status = 200
	return nil
}

func (m *Master) updateWorkerLive(workerID string) {
	m.workerMutex.Lock()
	defer m.workerMutex.Unlock()
	m.availableWorkers[workerID] = time.Now()
}

func (m *Master) finish(args *FinishRequest, reply *FinishResponse) error {

	workerID := args.WorkerID
	m.updateWorkerLive(workerID)
	taskID := args.ID

	m.taskMutex.Lock()
	if m.Tasks[taskID].Finished {
		reply.Status = 400
		return nil
	}

	m.Tasks[taskID].Finished = true
	m.taskMutex.Unlock()

	reply.Status = 200
	if args.WorkType == "reduce" {
		atomic.AddInt32(&m.succeedReduce, 1)
	}
	if args.WorkType == "map" {
		atomic.AddInt32(&m.succeedMap, 1)
		m.locationMutex.Lock()
		for _, path := range args.locations {
			tokens := strings.Split(path, "_")
			index, err := strconv.Atoi(tokens[len(tokens) - 1])
			if err != nil {
				log.Fatal("not right intermediate file path")
			}
			m.ReduceFileLocations[index] = append(m.ReduceFileLocations[index], path)
		}
		m.locationMutex.Unlock()

		if int(atomic.LoadInt32(&m.succeedMap)) == m.MapNum {
			for i := 0; i < m.ReducersNum; i++ {
				task := &WorkRecord{
					ID:        "reduce_" + strconv.Itoa(i),
					Type:      "reduce",
					locations: m.ReduceFileLocations[i],
				}
				m.Tasks[task.ID] = task
			}
		}
	}
	reply.Status = 200
	return nil
}

func (m *Master) Assign(args *AssignmentRequest, reply *AssignmentReply) error {

	if len(m.Tasks) == 0 {
		reply.Status = 404
		return nil
	}
	fmt.Printf("receiving the assign request from %v\n", args.WorkerID)
	m.updateWorkerLive(args.WorkerID)
	w := m.getNextTask()
	fmt.Printf("find the unassigned task: %v\n and its type %v\n", w.ID, w.Type)
	if w != nil {
		w.StartTime = time.Now()
		m.assignmentMutex.Lock()
		w.WorkerID = args.WorkerID
		m.Assigment[args.WorkerID] = append(m.Assigment[args.WorkerID], w)
		m.assignmentMutex.Unlock()
		reply.Status = 200
		reply.NReduce = m.ReducersNum
		reply.Record = WorkRecord{
			ID:        w.ID,
			WorkerID:  w.WorkerID,
			Type: w.Type,
			locations: w.locations,
			Assigned:  w.Assigned,
			Finished:  w.Finished,
			StartTime: w.StartTime,
		}
		return nil
	}
	fmt.Printf("now the assignment is\n")
	for k, v := range m.Assigment {
		for _, t := range v {
			fmt.Printf("the worker id is %+v and task id is %+v\n", k, t.ID)
		}
	}
	reply.Status = 400
	return nil
}

func (m *Master) checkWorkers() {

	ticker := time.Tick(100 * time.Microsecond)

	for {

		select {
		case <-ticker:
			m.workerMutex.Lock()
			for worker, updateTime := range m.availableWorkers {
				fmt.Printf("checking the worker %s at %+v and its update time is %+v\n", worker, time.Now(), updateTime)
				if time.Now().Sub(updateTime) > TIME_OUT {
					fmt.Printf("removing the worker %s at %+v and its update time is %+v\n", worker, time.Now(), updateTime)
					delete(m.availableWorkers, worker)
					for _, w := range m.Assigment[worker] {
						w.updateAssigned(false)
					}
				}
			}
			m.workerMutex.Unlock()
		case <-m.stopCheck:
			return
		}
	}
}

func (m *Master) checkTasks() {

	ticker := time.Tick(100 * time.Microsecond)

	for {
		select {
		case <-ticker:
			m.taskMutex.Lock()
			for _, w := range m.Tasks {
				fmt.Printf("checking the task %s at %+v and its start time is %+v\n", w.ID, time.Now(), w.StartTime)
				if time.Now().Sub(w.StartTime) > TIME_OUT {
					fmt.Printf("updating the task %s to unassigned %+v and its start time is %+v\n", w.ID, time.Now(), w.StartTime)
					w.updateAssigned(false)
				}
			}
			m.taskMutex.Unlock()
		case <-m.stopCheck:
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
	if m.succeedReduce == int32(m.ReducersNum) {
		close(m.stopCheck)
		return true
	} else {
		return false
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.ReducersNum = nReduce
	m.MapNum = len(files)
	m.stopCheck = make(chan struct{})
	m.Tasks = make(map[string]*WorkRecord)
	m.availableWorkers = make(map[string]time.Time)
	m.Assigment = make(map[string][]*WorkRecord)
	m.ReduceFileLocations = make(map[int][]string)
	for _, file := range files {
		record := &WorkRecord{
			ID:        file,
			Type:      "map",
			locations: []string{file},
		}
		m.Tasks[record.ID] = record
	}

	go m.checkWorkers()
	go m.checkTasks()
	m.server()
	return &m
}
