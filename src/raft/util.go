package raft

import (
	"fmt"
	"log"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Debugging
const Debug = false
const AllLog = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func PrintAllLog(m []map[int]interface{}) {
	if AllLog {
		for i := range m {
			fmt.Println("Member: ", i, "have log length: ", len(m[i]), "logs: ", m[i])
		}
	}
}

func retryFunc(retryTimes int, fn func() bool) func() {
	return func() {
		for i := 0; i < retryTimes; i++ {
			if ok := fn(); ok {
				return
			}
		}
	}
}

func getGID() int {
	if Debug {
		var buf [64]byte
		n := runtime.Stack(buf[:], false)
		// 得到id字符串
		idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
		id, err := strconv.Atoi(idField)
		if err != nil {
			panic(fmt.Sprintf("cannot get goroutine id: %v", err))
		}
		return id
	}
	return -1
}

func (rf *Raft) initSendRPCPool() {
	rf.sendRPCPool = sync.Pool{New: func() interface{} {
		ch := make(chan bool, 1)
		return ch
	}}
}

func (rf *Raft) sendRPCPoolGet() chan bool {
	return rf.sendRPCPool.Get().(chan bool)
}

func (rf *Raft) sendRPCPoolPut(ch chan bool) {
	if len(ch) > 0 {
		<-ch
	}
	rf.sendRPCPool.Put(ch)
}

func (rf *Raft) sendRPC(server int, method string, arg, reply interface{}, timeout ...time.Duration) bool {
	var to <-chan time.Time
	if len(timeout) == 0 {
		to = time.After(10 * time.Millisecond)
	} else {
		to = time.After(timeout[0])
	}
	done := rf.sendRPCPoolGet()
	defer rf.sendRPCPoolPut(done)
	//id := atomic.Int32{}
	over := atomic.Bool{}
	over.Store(false)
	go func() {
		ok := rf.peers[server].Call(method, arg, reply)
		if !over.Load() {
			done <- ok
		}
	}()
	select {
	case ok := <-done:
		return ok
	case <-to:
		over.Store(true)
		return false
	}
}
