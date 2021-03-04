package mr

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

const TIME_OUT = 10 * time.Second

type Master struct {
	// Your definitions here.
	Tasks map[string]*Task

	Assigment           map[string][]*Task
	availableWorkers    map[string]time.Time
	ReduceFileLocations map[int]map[string]bool

	ReducersNum     int
	MapNum          int
	succeedMap      int32
	succeedReduce   int32
	stopCheck       chan struct{}
	assignmentChan  chan AssigmentChanRequest
	finishChan      chan TaskFinishRequest
	heartbeatChan   chan HeartbeatRequest
}

type AssigmentChanRequest struct {
	request  AssignmentRequest
	response *chan *Task
}

type TaskFinishRequest struct {
	request  FinishRequest
	response *chan int
}

func (m *Master) getNextTask() *Task {
	//m.taskMutex.Lock()
	//defer m.taskMutex.Unlock()
	for _, v := range m.Tasks {
		if !v.Finished && !v.getAssigned() {
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
	m.heartbeatChan <- *args
	reply.Status = 200
	return nil
}

func (m *Master) updateWorkerLive(workerID string) {
	m.availableWorkers[workerID] = time.Now()
}

func (m *Master) Finish(args *FinishRequest, reply *FinishResponse) error {

	m.updateWorkerLive(args.WorkerID)
	response := make(chan int)
	m.finishChan <- TaskFinishRequest{
		request:  *args,
		response: &response,
	}
	ticker := time.Tick(5 * time.Second)
	select {
	case r := <-response:
		reply.Status = r
		return nil
	case <-ticker:
		reply.Status = 500
		return nil
	}
}

func (m *Master) Assign(args *AssignmentRequest, reply *AssignmentReply) error {

	fmt.Printf("receiving the assign request from %v\n", args.WorkerID)
	m.updateWorkerLive(args.WorkerID)
	response := make(chan *Task)
	ticker := time.Tick(5 * time.Second)
	m.assignmentChan <- AssigmentChanRequest{request: *args, response: &response}
	select {
	case w := <-response:
		if w == nil {
			reply.Status = 404
		} else {
			reply.Status = 200
			reply.NReduce = m.ReducersNum
			reply.Record = Task{
				ID:        w.ID,
				WorkerID:  w.WorkerID,
				Type:      w.Type,
				Assigned:  w.Assigned,
				Finished:  w.Finished,
				Locations: w.Locations,
				StartTime: w.StartTime,
				Partition: w.Partition,
			}
		}
		return nil
	case <-ticker:
		reply.Status = 500
		return nil
	}
}

func (m *Master) checkWorkerOneTime() {
	for worker, updateTime := range m.availableWorkers {
		if time.Now().Sub(updateTime) > TIME_OUT {
			fmt.Printf("removing the worker %s at %+v and its update time is %+v\n", worker, time.Now(), updateTime)
			delete(m.availableWorkers, worker)
			for _, w := range m.Assigment[worker] {
				w.updateAssigned(false)
			}
		}
	}
}

func (m *Master) checkTasksOneTime() {
	for _, w := range m.Tasks {
		if !w.Finished && w.Assigned && time.Now().Sub(w.StartTime) > TIME_OUT {
			fmt.Printf("updating the task %s to unassigned %+v and its start time is %+v\n", w.ID, time.Now(), w.StartTime)
			w.updateAssigned(false)
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
	go m.EventLoop()
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	fmt.Printf("Done, the succeed reduce is %+v\n", m.succeedReduce)
	if m.succeedReduce == int32(m.ReducersNum) {
		close(m.stopCheck)
		return true
	} else {
		return false
	}
}

func (m *Master) EventLoop() {
	workerTicker := time.Tick(2 * time.Second)
	taskTicker := time.Tick(2 * time.Second)
	for {
		select {
		case <-workerTicker:
			m.checkWorkerOneTime()
		case <-taskTicker:
			m.checkWorkerOneTime()
		case heartbeatRequest := <-m.heartbeatChan:
			m.updateWorkerLive(heartbeatRequest.WorkerID)
		case assignRequest := <-m.assignmentChan:
			responseChan := *assignRequest.response
			if len(m.Tasks) == 0 {
				responseChan <- nil
			} else {
				w := m.getNextTask()
				if w == nil {
					responseChan <- nil
				} else {
					w.StartTime = time.Now()
					w.updateAssigned(true)
					workerID := assignRequest.request.WorkerID
					w.WorkerID = workerID
					m.Assigment[workerID] = append(m.Assigment[workerID], w)
					responseChan <- w
				}
			}
		case finishRequest := <-m.finishChan:
			responseChan := *finishRequest.response
			args := finishRequest.request
			taskID := args.ID
			if m.Tasks[taskID].Finished {
				responseChan <- 400
			} else {
				responseChan <- 200
				m.Tasks[taskID].Finished = true
				if args.WorkType == "reduce" {
					atomic.AddInt32(&m.succeedReduce, 1)
				}
				if args.WorkType == "map" {
					atomic.AddInt32(&m.succeedMap, 1)
					m.updateReduceLocations(args.Locations)
					if int(atomic.LoadInt32(&m.succeedMap)) == m.MapNum {
						m.addReduceTasks()
					}
				}
			}
		}
	}

}

func (m *Master) updateReduceLocations(locations []string) {
	for _, path := range locations {
		tokens := strings.Split(path, "_")
		index, err := strconv.Atoi(tokens[len(tokens)-1])
		if err != nil {
			log.Fatal("not right intermediate file path")
		}
		if _, ok := m.ReduceFileLocations[index]; !ok {
			m.ReduceFileLocations[index] = make(map[string]bool)
		}
		m.ReduceFileLocations[index][path] = true
		fmt.Printf("the reduce location files are %+v\n", m.ReduceFileLocations)
	}
}

func (m *Master) addReduceTasks() {
	for i := 0; i < m.ReducersNum; i++ {
		locations := make([]string, 0, len(m.ReduceFileLocations[i]))
		for k, _ := range m.ReduceFileLocations[i] {
			locations = append(locations, k)
		}
		if len(locations) == 0 {
			atomic.AddInt32(&m.succeedReduce, 1)
			continue
		}
		temp := i
		task := &Task{
			ID:        "reduce_" + strconv.Itoa(i),
			Type:      "reduce",
			Partition: temp,
			Locations: locations,
		}
		m.Tasks[task.ID] = task
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
	m.Tasks = make(map[string]*Task)
	m.availableWorkers = make(map[string]time.Time)
	m.Assigment = make(map[string][]*Task)
	m.ReduceFileLocations = make(map[int]map[string]bool)
	for _, file := range files {
		record := &Task{
			ID:        file,
			Type:      "map",
			Locations: []string{file},
		}
		m.Tasks[record.ID] = record
	}
	fmt.Printf("the task length is %v\n", len(m.Tasks))
	m.server()
	return &m
}
