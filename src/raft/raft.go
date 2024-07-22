package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//   每有一个条目提交到log之后，raft应该发送一条ApplyMsg 到 ch中

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Raft struct {
	mu          sync.RWMutex        // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	term        atomic.Int32
	isLeader    atomic.Bool
	isCandidate atomic.Bool
	lastHeart   atomic.Int64 // millisecond
	sendRPCPool sync.Pool
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return int(rf.term.Load()), rf.isLeader.Load()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

func (rf *Raft) sendRPC(server int, method string, arg, reply interface{}) bool {
	timeout := time.After(10 * time.Millisecond)
	done := rf.sendRPCPoolGet()
	defer rf.sendRPCPoolPut(done)
	go func() {
		done <- rf.peers[server].Call(method, arg, reply)
	}()

	select {
	case ok := <-done:
		return ok
	case <-timeout:
		return false
	}
}

type RequestVoteArgs struct {
	Term        int
	CandidateID int
	// Your data here (3A, 3B).
}

type RequestVoteReply struct {
	GrantVote bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	vote := func() {
		reply.GrantVote = true
		DPrintf("%d vote to %d\n", rf.me, args.CandidateID)
	}

	term := int(rf.term.Load())
	if term > args.Term-1 {
		reply.GrantVote = false
		DPrintf("%d号拒绝给%d号投票，因为%d号的term>=arg.Term\n", rf.me, args.CandidateID, rf.me)
		return
	}
	if rf.isLeader.Load() {
		reply.GrantVote = false
		DPrintf("%d号拒绝给%d号投票，因为%d号是Leader\n", rf.me, args.CandidateID, rf.me)
		return
	}
	if rf.isCandidate.Load() {
		if term != args.Term-1 {
			vote()
			return
		}
		if rf.me < args.CandidateID {
			reply.GrantVote = false
			DPrintf("%d号拒绝给%d号投票，因为%d的序列更靠前\n", rf.me, args.CandidateID, rf.me)
			return
		}
	}
	vote()
	// Your code here (3A, 3B).
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("%d向%d号发送了投票请求", rf.me, server)
	ok := rf.sendRPC(server, "Raft.RequestVote", args, reply)
	if ok {
		DPrintf("%d向%d号请求投票成功", rf.me, server)
	} else {
		DPrintf("%d向%d号请求投票失败", rf.me, server)
	}
	return ok
}

type AppendEntriesArgs struct {
	Term int
}

type AppendEntriesReply struct {
}

func (rf *Raft) AppendEntries(arg *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.term.Store(int32(arg.Term))
	rf.lastHeart.Store(time.Now().UnixMilli())
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.sendRPC(server, "Raft.AppendEntries", args, reply)
	if !ok {
		DPrintf("%d发送心跳给%d失败", rf.me, server)
	}
	return ok
}

func (rf *Raft) sendHeartBeat() {
	for rf.isLeader.Load() {
		var wg sync.WaitGroup
		var loseConnectNum atomic.Int32
		DPrintf("%d号开始发送心跳", rf.me)
		for i := range rf.peers {
			if i != rf.me {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					arg := &AppendEntriesArgs{Term: int(rf.term.Load())}
					reply := &AppendEntriesReply{}
					if ok := rf.sendAppendEntries(i, arg, reply); !ok {
						loseConnectNum.Add(1)
					}
				}(i)
			}
		}
		wg.Wait()
		if int(loseConnectNum.Load()) >= len(rf.peers)-1 { // 如果这个leader除了自己谁都联系不上
			rf.isLeader.Store(false)
			DPrintf("%d号Leader已宕机", rf.me)
			return
		}
		time.Sleep(heartBeatInterval)
	}
}

func (rf *Raft) loseHeart() bool {
	if rf.isLeader.Load() {
		return false
	}
	return time.Now().UnixMilli()-rf.lastHeart.Load() > tolerableHeartBeatInterval.Milliseconds()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	DPrintf("%d号Start", rf.me)

	// Your code here (3B).

	return index, int(rf.term.Load()), rf.isLeader.Load()
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.isLeader.Store(false)
	DPrintf("%d号被kill\n", rf.me)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

const heartBeatInterval = 100 * time.Millisecond         // 150
const tolerableHeartBeatInterval = 3 * heartBeatInterval // 5

func (rf *Raft) ticker() {
	for rf.killed() == false {
		if !rf.loseHeart() { // 没有超过可容忍时间或者此节点是leader就跳过选举
			time.Sleep(10 * time.Millisecond)
			continue
		}
		DPrintf("%d号开始选举\n", rf.me)
		rf.isCandidate.Store(true)
		newterm := int(rf.term.Load() + 1)
		var vote int
		for i := range rf.peers {
			if i == rf.me {
				vote++
				continue
			}
			arg := &RequestVoteArgs{Term: newterm, CandidateID: rf.me}
			reply := &RequestVoteReply{}
			if ok := rf.sendRequestVote(i, arg, reply); !ok {
				continue
			}
			if !reply.GrantVote {
				vote = 0
				rf.term.Store(int32(newterm))
				break
			}
			vote++
		}
		DPrintf("%d号投票结束", rf.me)
		if vote > len(rf.peers)/2 {
			rf.isLeader.Store(true) // 竞选成功,下一步发送心跳
			rf.term.Store(int32(newterm))
			go rf.sendHeartBeat()
			DPrintf("%d号获得了%d票,成为Leader\n", rf.me, vote)
		}
		rf.isCandidate.Store(false)
		ms := 150 + (rand.Int63() % 250)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.term.Store(0)
	rf.isLeader.Store(false)
	rf.isCandidate.Store(false)
	rf.lastHeart.Store(time.Now().UnixMilli())

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.initSendRPCPool()
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
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
