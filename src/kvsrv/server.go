package kvsrv

import (
	"fmt"
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu        sync.Mutex
	kvs       map[string]string
	haveDone  map[int64]struct{} // last taskID
	tid2Value map[int64]string   // TaskID -> value
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()

	if v, ok := kv.kvs[args.Key]; ok {
		reply.Value = v
		kv.mu.Unlock()
		DPrintf("key:%s is exist,value:%s\n", args.Key, v)
		return
	}

	kv.mu.Unlock()
	DPrintf("key:%s is not exist\n", args.Key)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if kv.taskHaveDone(args.TaskID) {
		kv.mu.Unlock()
		return
	}

	kv.kvs[args.Key] = args.Value
	kv.updateLastTaskID(args.TaskID)
	kv.mu.Unlock()
	DPrintf("put key:%s value: %s\n", args.Key, args.Value)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if kv.taskHaveDone(args.TaskID) {
		reply.Value = kv.tid2Value[args.TaskID]
		kv.mu.Unlock()
		return
	}

	DPrintf("append key:%s value: %s\n", args.Key, args.Value)
	if v, ok := kv.kvs[args.Key]; ok {
		reply.Value = v
		kv.tid2Value[args.TaskID] = v
		kv.kvs[args.Key] = fmt.Sprintf("%s%s", v, args.Value)
		kv.updateLastTaskID(args.TaskID)
		kv.mu.Unlock()
		return
	}

	reply.Value = ""
	kv.tid2Value[args.TaskID] = ""
	kv.kvs[args.Key] = args.Value
	kv.updateLastTaskID(args.TaskID)
	kv.mu.Unlock()
}

func (kv *KVServer) Close(arg *CloseArgs, reply *CloseArgs) {
	kv.mu.Lock()
	delete(kv.haveDone, arg.TaskID)
	delete(kv.tid2Value, arg.TaskID)
	kv.mu.Unlock()
}

func (kv *KVServer) taskHaveDone(taskID int64) bool {
	if _, ok := kv.haveDone[taskID]; ok {
		return true
	}
	return false
}

func (kv *KVServer) updateLastTaskID(taskID int64) {
	kv.haveDone[taskID] = struct{}{}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kvs = make(map[string]string)
	kv.haveDone = make(map[int64]struct{})
	kv.tid2Value = make(map[int64]string)
	return kv
}
