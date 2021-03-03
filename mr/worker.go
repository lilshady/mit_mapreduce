package mr

import (
	"bufio"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"sync/atomic"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//

type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func randString(n int) string {
	const alphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, n)
	rand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = alphanum[b%byte(len(alphanum))]
	}
	return string(bytes)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	worker := MRWorker{
		mapFunc:    mapf,
		reduceFunc: reducef,
		id:         randString(16),
	}

	fmt.Println("about to start the worker")
	worker.start()
	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
}

type MRWorker struct {
	mapFunc    func(string, string) []KeyValue
	reduceFunc func(string, []string) string
	id         string
	Running    int32
}

func (worker *MRWorker) start() {

	atomic.StoreInt32(&worker.Running, 1)
	go worker.sendHeartBeat()

	for atomic.LoadInt32(&worker.Running) == 1 {

		task, reduceNumber, err := worker.getWorkFromMaster()

		if task == nil || err != nil {
			time.Sleep(1 * time.Second)
			fmt.Println(err)
			continue
		}
		fmt.Printf("get the task locations %+v and type %+v and is is %+v and reduce number is %v\n", task.Locations, task.Type, task.ID, reduceNumber)

		var request FinishRequest
		var reply FinishResponse
		if task.Type == "map" {
			request = worker.handleMapTask(task, reduceNumber)
		}

		if task.Type == "reduce" {
			request = worker.handleReduceTask(task)
		}
		succeed := false
		for !succeed {
			succeed = call("Master.Finish", &request, &reply)
			if reply.Status == 200 {
				break
			}
		}
	}
}

func (worker *MRWorker) handleMapTask(task *Task, reduceNumber int) FinishRequest {

	filePath := task.Locations[0]
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("cannot open %v", filePath)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filePath)
	}
	defer file.Close()
	fileCache := make(map[int]*os.File)
	locations := make([]string, 0)
	kva := worker.mapFunc(filePath, string(content))
	for _, kv := range kva {
		shard := ihash(kv.Key) % reduceNumber
		file, ok := fileCache[shard]
		if !ok {
			path := worker.id + "_map_" + task.ID + strconv.FormatInt(time.Now().UnixNano()/1000000, 10) +"_"+ strconv.Itoa(shard)
			locations = append(locations, path)
			file, err = createFileIfNotExist(path)
			if err != nil {
				log.Fatal("unable to open the file")
			}
			fileCache[shard] = file
		}
		c, err := json.Marshal(kv)
		if err != nil {
			continue
		}
		file.Write(c)
		file.WriteString("\n")
	}
	return FinishRequest{
		ID:        task.ID,
		WorkerID:  worker.id,
		WorkType:  "map",
		Locations: locations,
	}
}

func (worker *MRWorker) handleReduceTask(task *Task) FinishRequest {
	locations := task.Locations
	data := make([]KeyValue, 0)
	for _, filename := range locations {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}
			kv := &KeyValue{}
			err := json.Unmarshal(line, kv)
			if err != nil {
				fmt.Printf("unable to marsh %v\n", string(line))
			}
			data = append(data, *kv)
		}
	}
	sort.Sort(ByKey(data))
	oname := "mr-out-" + strconv.Itoa(task.Partition)
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	i := 0
	for i < len(data) {
		j := i + 1
		for j < len(data) && data[j].Key == data[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, data[k].Value)
		}
		output := worker.reduceFunc(data[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", data[i].Key, output)

		i = j
	}

	return FinishRequest{
		ID:        task.ID,
		WorkerID:  worker.id,
		WorkType:  "reduce",
		Locations: locations,
	}
}

func createFileIfNotExist(filename string) (*os.File, error) {

	f, err := os.OpenFile(filename,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	return f, err
}

func (worker *MRWorker) getWorkFromMaster() (*Task, int, error) {

	request := AssignmentRequest{WorkerID: worker.id}
	reply := &AssignmentReply{}
	succeed := call("Master.Assign", &request, reply)
	if !succeed {
		return nil, 0, errors.New("unable to get new task as failed")
	}
	if reply.Status != 200 {
		return nil, 0, errors.New("Unable to get new task " + strconv.Itoa(reply.Status))
	}
	return &reply.Record, reply.NReduce, nil
}

func (worker *MRWorker) sendHeartBeat() {
	ticker := time.Tick(1 * time.Second)
	failed := 0
	for {
		select {
		case <-ticker:
			request := HeartbeatRequest{WorkerID: worker.id}
			response := HeartbeatResponse{}
			result := call("Master.HeartBeat", &request, &response)
			if !result {
				failed = failed + 1
			}
			if failed > 10 {
				atomic.StoreInt32(&worker.Running, 0)
				return
			}
		}
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//sockname := masterSock()
	//c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}
