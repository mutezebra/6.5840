package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	keys sync.Map
	
	kvs map[string]*Item
	mu  sync.Mutex
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		mu:   sync.Mutex{},
		keys: sync.Map{},
		kvs:  make(map[string]*Item),
	}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	reply.Err = rpc.OK
	if _, ok := kv.keys.Load(args.Key); !ok {
		reply.Err = rpc.ErrNoKey
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	item := kv.kvs[args.Key]
	reply.Version = item.Version
	reply.Value = item.V
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	reply.Err = rpc.OK

	if _, ok := kv.keys.Load(args.Key); !ok {
		if args.Version == rpc.ZERO {
			kv.create(args.Key, args.Value)
			reply.Version = 1
			return
		}
		reply.Err = rpc.ErrNoKey
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	item := kv.kvs[args.Key]
	if item.Version != args.Version {
		reply.Version = item.Version
		reply.Err = rpc.ErrVersion
		return
	}

	item.V = args.Value
	item.Version++
	reply.Version = item.Version
}

var nilV = struct{}{}

func (kv *KVServer) create(key, val string) {
	kv.mu.Lock()
	kv.keys.Store(key, nilV)
	kv.kvs[key] = NewItem(key, val)
	kv.mu.Unlock()
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
