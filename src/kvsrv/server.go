package kvsrv

import (
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
	defer kv.mu.Unlock()

	if v, ok := kv.kvs[args.Key]; ok {
		reply.Value = v
		DPrintf("key:%s is exist,value:%s\n", args.Key, v)
		return
	}

	DPrintf("key: %s is not exist\n", args.Key)
	reply.Value = ""
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.taskHaveDone(args.TaskID) {
		return
	}

	DPrintf("put key:%s value: %s\n", args.Key, args.Value)
	kv.kvs[args.Key] = args.Value
	kv.updateLastTaskID(args.TaskID)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.taskHaveDone(args.TaskID) {
		reply.Value = kv.tid2Value[args.TaskID]
		return
	}

	DPrintf("append key:%s value: %s\n", args.Key, args.Value)
	if v, ok := kv.kvs[args.Key]; ok {
		reply.Value = v
		kv.tid2Value[args.TaskID] = reply.Value
		v = v + args.Value
		kv.kvs[args.Key] = v
		kv.updateLastTaskID(args.TaskID)
		return
	}

	reply.Value = ""
	kv.tid2Value[args.TaskID] = reply.Value
	kv.kvs[args.Key] = args.Value
	kv.updateLastTaskID(args.TaskID)
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
