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
	"6.5840/labgob"
	"bytes"
	"log"
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
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	applyChan chan ApplyMsg

	me          int         // this peer's index into peers[]
	dead        atomic.Bool // set by Kill()
	term        atomic.Int32
	isLeader    atomic.Bool
	isCandidate atomic.Bool
	lastHeart   atomic.Int64 // millisecond

	manager          *TaskManager
	submitTp         int
	entries          []interface{} // entries
	readyEntries     []interface{}
	readyEntriesTerm int
	nextCommitIndex  int
	sucMap           map[int]bool

	startIndex  atomic.Int32
	persister   *Persister // Object to hold this peer's persisted state
	sendRPCPool sync.Pool
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(int(rf.term.Load()))
	_ = e.Encode(rf.entries)
	_ = e.Encode(rf.readyEntriesTerm)
	_ = e.Encode(rf.nextCommitIndex)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term, nIndex int
	var readyEntriesTerm int
	var entries []interface{}
	if d.Decode(&term) != nil ||
		d.Decode(&entries) != nil ||
		d.Decode(&readyEntriesTerm) != nil ||
		d.Decode(&nIndex) != nil {
		log.Fatalf("read persist failed")
	} else {
		rf.term.Store(int32(term))
		rf.entries = entries
		rf.readyEntriesTerm = readyEntriesTerm
		rf.nextCommitIndex = nIndex
	}
	DPrintf("%d read entries is %v,term is %d,", rf.me, entries, term)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
}

func (rf *Raft) Start(command interface{}) (int, int, bool) { // commitIndex,currentTerm,isLeader
	index := -1
	term := int(rf.term.Load())
	if !rf.isLeader.Load() || rf.dead.Load() {
		return index, term, false
	}
	rf.mu.Lock()
	rf.manager.AddTask(rf.submitTp, rf.submitLog(command))
	rf.mu.Unlock()
	index = int(rf.startIndex.Load()) + 1
	rf.startIndex.Add(1)
	return index, term, true
}

func (rf *Raft) Kill() {
	rf.manager.ClearTaskQueue(ClearAll)
	rf.dead.Store(true)
	rf.isLeader.Store(false)
	DPrintf("%d have been killed", rf.me)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	return rf.dead.Load()
}

const heartBeatInterval = 150 * time.Millisecond         // 150
const tolerableHeartBeatInterval = 3 * heartBeatInterval // 5

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead.Store(false)
	rf.term.Store(0)
	rf.isLeader.Store(false)
	rf.isCandidate.Store(false)
	rf.lastHeart.Store(time.Now().UnixMilli())

	rf.manager = NewTaskManager(10)
	rf.submitTp = rf.manager.NewTaskType()
	rf.entries = make([]interface{}, 0)
	rf.startIndex.Store(0)
	rf.applyChan = applyCh
	rf.manager.AddTask(RandomQueue, rf.sendMsg(new(ApplyMsg), true, nil, 0))
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.initSendRPCPool()
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// sendMsg will build a ApplyMsg across the
// arguments which you passed, if msg is nil,
func (rf *Raft) sendMsg(msg *ApplyMsg, valid bool, cmd interface{}, cmdI int) func() {
	if msg == nil {
		msg = &ApplyMsg{
			CommandValid:  valid,
			Command:       cmd,
			CommandIndex:  cmdI,
			SnapshotValid: false,
			Snapshot:      nil,
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
	}
	return func() {
		rf.applyChan <- *msg
	}
}
