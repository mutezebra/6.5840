package kvsrv

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	mu     sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.mu = sync.Mutex{}
	return ck
}

func (ck *Clerk) getTaskID() int64 {
	ck.mu.Lock()
	id := time.Now().UnixNano()
	ck.mu.Unlock()
	return id
}

func (ck *Clerk) Get(key string) string {
	arg := &GetArgs{Key: key}
	reply := &GetReply{}

	ck.mu.Lock()
	for !ck.server.Call("KVServer.Get", arg, reply) {
	}
	ck.mu.Unlock()

	return reply.Value
}

func (ck *Clerk) PutAppend(key string, value string, op string) string {
	arg := &PutAppendArgs{Key: key, Value: value, TaskID: ck.getTaskID()}
	reply := &PutAppendReply{}

	if op == "Put" {
		ck.mu.Lock()
		for !ck.server.Call("KVServer.Put", arg, reply) {
		}
		for !ck.server.Call("KVServer.Close", &CloseArgs{TaskID: arg.TaskID}, &CloseReply{}) {
		}
		ck.mu.Unlock()
	} else if op == "Append" {
		ck.mu.Lock()
		for !ck.server.Call("KVServer.Append", arg, reply) {
		}
		for !ck.server.Call("KVServer.Close", &CloseArgs{TaskID: arg.TaskID}, &CloseReply{}) {
		}
		ck.mu.Unlock()
	}

	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
