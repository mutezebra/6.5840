package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
	period  int64
}

// 辅助函数：关闭文件并记录错误
func closeFile(file *os.File) {
	if err := file.Close(); err != nil {
		log.Printf("Failed to close file: %v", err)
	}
}

// 辅助函数：创建临时文件并处理错误
func createTempFile() (*os.File, error) {
	temp, err := os.CreateTemp("", "%0-%v-%v")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	return temp, nil
}

// 请求并处理 Map 任务的发布
func (w *worker) requestMapTask() (*PublishMapWorkReply, bool) {
	pubArg := &PublishMapWorkArgs{}
	pubReply := &PublishMapWorkReply{}
	if !call(publishMapRPC, pubArg, pubReply) {
		return nil, false
	}
	w.period = pubReply.Period
	if w.period == CompletePeriod || pubReply.Filepath == "" {
		return nil, false
	}
	return pubReply, true
}

// 读取 Map 任务的文件内容
func readMapFile(filepath string) ([]byte, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filepath, err)
	}
	defer closeFile(file)

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", filepath, err)
	}
	return data, nil
}

// 处理 Map 任务的中间文件生成
func processMapIntermediateFiles(pubReply *PublishMapWorkReply, kvs []KeyValue) []string {
	middlePaths := make([]string, pubReply.NReduce)
	for i := 0; i < pubReply.NReduce; i++ {
		temp, err := createTempFile()
		if err != nil {
			panic(err)
		}

		enc := json.NewEncoder(temp)
		for _, kv := range kvs {
			if ihash(kv.Key)%pubReply.NReduce == i {
				if err = enc.Encode(kv); err != nil {
					log.Printf("Failed to encode key-value pair: %v", err)
				}
			}
		}

		if err := temp.Sync(); err != nil {
			log.Printf("Failed to sync temp file: %v", err)
		}

		middlepath := fmt.Sprintf(middleFileFormat, pubReply.Index, i)
		if err = os.Rename(temp.Name(), middlepath); err != nil {
			log.Printf("Failed to rename temp file to %s: %v", middlepath, err)
		}
		middlePaths[i] = middlepath
		closeFile(temp)
	}
	return middlePaths
}

// 完成 Map 任务并更新周期
func (w *worker) completeMapTask(pubReply *PublishMapWorkReply, middlePaths []string) {
	comArg := &CompleteMapWorkArgs{
		Index:     pubReply.Index,
		Filepaths: middlePaths,
	}
	comReply := &CompleteMapWorkReply{}
	if !call(completeMapRPC, comArg, comReply) {
		return
	}

	w.period = comReply.Period
}

func (w *worker) mapTask() {
	pubReply, ok := w.requestMapTask()
	if !ok {
		return
	}

	data, err := readMapFile(pubReply.Filepath)
	if err != nil {
		log.Fatal(err)
	}

	kvs := w.mapf(pubReply.Filepath, string(data))
	middlePaths := processMapIntermediateFiles(pubReply, kvs)
	w.completeMapTask(pubReply, middlePaths)
}

// 请求并处理 Reduce 任务的发布
func (w *worker) requestReduceTask() (*PublishReduceWorkReply, bool) {
	pubArgs := &PublishReduceWorkArgs{}
	pubReply := &PublishReduceWorkReply{}
	if !call(publicReduceRPC, pubArgs, pubReply) {
		return nil, false
	}
	w.period = pubReply.Period
	if w.period == CompletePeriod || pubReply.Filepaths == nil {
		return nil, false
	}
	return pubReply, true
}

// 读取 Reduce 任务的所有文件内容
func readReduceFiles(filepaths []string) map[string][]string {
	kvs := make(map[string][]string)
	for _, filepath := range filepaths {
		file, err := os.Open(filepath)
		if err != nil {
			log.Printf("open file %s failed: %v", filepath, err)
			continue
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
		closeFile(file)
	}
	return kvs
}

// 处理 Reduce 任务的输出文件生成
func processReduceOutputFile(pubReply *PublishReduceWorkReply, w *worker, kvs map[string][]string) string {
	temp, err := createTempFile()
	if err != nil {
		panic(err)
	}
	defer closeFile(temp)

	var keys []string
	for key := range kvs {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		output := w.reducef(key, kvs[key])
		if _, err := temp.WriteString(fmt.Sprintf("%v %v\n", key, output)); err != nil {
			log.Printf("Failed to write to temp file: %v", err)
		}
	}

	outpath := fmt.Sprintf(outputFileFormat, pubReply.ReduceSequence)
	if err := os.Rename(temp.Name(), outpath); err != nil {
		log.Printf("Failed to rename temp file to %s: %v", outpath, err)
	}
	if err := temp.Sync(); err != nil {
		log.Printf("Failed to sync temp file: %v", err)
	}
	return outpath
}

// 完成 Reduce 任务并更新周期
func (w *worker) completeReduceTask(pubReply *PublishReduceWorkReply) {
	comArg := &CompleteReduceWorkArgs{ReduceSequence: pubReply.ReduceSequence}
	comReply := &CompleteReduceWorkReply{}
	if !call(completeReduceRPC, comArg, comReply) {
		return
	}
	w.period = comReply.Period
}

func (w *worker) reduceTask() {
	pubReply, ok := w.requestReduceTask()
	if !ok {
		return
	}

	kvs := readReduceFiles(pubReply.Filepaths)
	processReduceOutputFile(pubReply, w, kvs)
	w.completeReduceTask(pubReply)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	w := worker{mapf: mapf, reducef: reducef, period: MapPeriod}

	for {
		// Map 时期
		if w.period == MapPeriod {
			w.mapTask()
		}

		if w.period == ReducePeriod {
			w.reduceTask()
		}

		if w.period == CompletePeriod {
			return
		}
		time.Sleep(workerSleepInterval)
	}
}

func CallExample() {
	args := ExampleArgs{}
	args.X = 99
	reply := ExampleReply{}
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatalf("dialing %s: %v", sockname, err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Printf("RPC call %s failed: %v", rpcname, err)
	return false
}
