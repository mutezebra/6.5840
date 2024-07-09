package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"time"
)
import "net/rpc"
import "hash/fnv"

type KeyValue struct {
	Key   string
	Value string
}

func ihash(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var Client *rpc.Client

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	Client = c
	for {
		time.Sleep(HeartInterval)
		reply := getTask()
		if reply.Period == CompletePeriod {
			break
		}
		if period := doTask(&reply.Task, mapf, reducef); period == CompletePeriod {
			break
		}
	}

	_ = c.Close()
}

func getTask() *ReqTaskReply {
	arg := ReqTaskArgs{}
	reply := ReqTaskReply{}
	if err := call("Coordinator.ReceiveTaskReq", &arg, &reply); err != nil {
		log.Fatalf("failed when call `Coordinator.ReceiveTaskReq`,error: %v", err)
	}
	return &reply
}

func doTask(task *Task, mapf func(string, string) []KeyValue, reducef func(string, []string) string) Period {
	if task.Period == MapPeriod {
		middlePath := DoMapTask(mapf, task.Files[0], task.Index, task.ReduceNum)
		return reportTask("Coordinator.ReceiveTaskResp", task.Index, middlePath)
	}
	if task.Period == ReducePeriod {
		DoReduceTask(reducef, task.Files, task.Index)
		return reportTask("Coordinator.ReceiveTaskResp", task.Index, nil)
	}
	if task.Period == CompletePeriod {
		return CompletePeriod
	}

	log.Fatalf("Unknownd period")
	return CompletePeriod
}

func DoMapTask(mapf func(string, string) []KeyValue, filepath string, index int, reduceNum int) []string {
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("failed when open %s file,error: %v", filepath, err)
	}
	body, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("failed when read from %s file,error: %v", filepath, err)
	}
	_ = file.Close()
	kvs := mapf(filepath, string(body))
	middlePaths := make([]string, reduceNum)
	for i := 0; i < reduceNum; i++ {
		middleFileName := getMiddleFileName(index, i)
		middlePaths = append(middlePaths, middleFileName)
		f, _ := os.Create(middleFileName)
		enc := json.NewEncoder(f)
		for _, kv := range kvs {
			if ihash(kv.Key)%reduceNum == i {
				_ = enc.Encode(&kv)
			}
		}
		_ = f.Close()
	}
	return middlePaths
}

func DoReduceTask(reducef func(string, []string) string, filepaths []string, index int) {
	kvs := make(map[string][]string)
	for _, path := range filepaths {
		file, err := os.Open(path)
		if err != nil {
			log.Fatalf("failed when open %s file,error: %v", path, err)
		}
		dec := json.NewDecoder(file)
		var kv KeyValue
		for dec.Decode(&kv) == nil {
			if _, ok := kvs[kv.Key]; !ok {
				kvs[kv.Key] = make([]string, 0)
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
			kv = KeyValue{}
		}
		_ = file.Close()
	}

	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	filaname := fmt.Sprintf("mr-out-%d", index)
	f, err := os.Create(filaname)
	if err != nil {
		log.Fatalf("failed when create %s file,error: %v", filaname, err)
	}
	for _, k := range keys {
		output := reducef(k, kvs[k])
		_, _ = f.WriteString(fmt.Sprintf("%v %v\n", k, output))
	}
	_ = f.Close()
}

func reportTask(rpcname string, index int, middleFilePaths []string) Period {
	arg := TaskRespArgs{
		Index:          index,
		TaskSuccess:    true,
		MiddleFilePath: middleFilePaths,
	}
	reply := TaskRespReply{}
	_ = call(rpcname, &arg, &reply)
	return reply.Period
}

func getMiddleFileName(mapIndex, reduceIndex int) string {
	return fmt.Sprintf("mr-%d-%d", mapIndex, reduceIndex)
}

func call(rpcname string, args interface{}, reply interface{}) error {
	return Client.Call(rpcname, args, reply)
}
