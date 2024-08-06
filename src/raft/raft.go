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
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	applyChan chan ApplyMsg

	me          int         // this peer's index into peers[]
	dead        atomic.Bool // set by Kill()
	term        atomic.Int32
	isLeader    atomic.Bool
	isCandidate atomic.Bool
	lastHeart   atomic.Int64 // millisecond

	manager           *TaskManager
	submitTp          int
	entries           []interface{} // entries
	readyEntries      []interface{}
	readyEntriesIndex int
	nextCommitIndex   atomic.Int32
	startIndex        atomic.Int32
	persister         *Persister // Object to hold this peer's persisted state
	sendRPCPool       sync.Pool
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
	}
	term := int(rf.term.Load())
	if term > args.Term-1 {
		reply.GrantVote = false
		return
	}
	if rf.isLeader.Load() {
		reply.GrantVote = false
		return
	}
	if rf.isCandidate.Load() {
		if term != args.Term-1 {
			rf.isCandidate.Store(false)
			vote()
			return
		}
		if rf.me < args.CandidateID {
			reply.GrantVote = false
			return
		}
	}
	vote()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.sendRPC(server, "Raft.RequestVote", args, reply)
}

type AppendEntriesArgs struct {
	Term              int
	Entries           []interface{}
	LeaderCommitIndex int
	Replenish         bool
}

type AppendEntriesReply struct {
	Success            bool
	ExpectEntriesStart int
}

func (rf *Raft) AppendEntries(arg *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.term.Store(int32(arg.Term))
	rf.lastHeart.Store(time.Now().UnixMilli())
	if rf.isLeader.Load() { // 如果我是leader,但还是接收到了请求，那说明还存在其他Leader
		if int(rf.nextCommitIndex.Load()) < arg.LeaderCommitIndex {
			rf.isLeader.Store(false)
		}
	}
	reply.Success = true

	rf.mu.Lock() // 避免同时处理多个AppendEntries
	defer rf.mu.Unlock()
	if arg.LeaderCommitIndex > int(rf.nextCommitIndex.Load()) && !arg.Replenish { // 如果arg的index更大，那说明需要commit了
		if len(rf.entries)+len(rf.readyEntries) < arg.LeaderCommitIndex { // 如果commit的entries和准备提交的entries加起来数目对不上，那就说明缺失了一部分
			reply.Success = false
			reply.ExpectEntriesStart = len(rf.entries)
			return
		}
		// 如果没有缺少数据的话，就commit
		rf.entries = append(rf.entries, rf.readyEntries...)
		rf.readyEntries = nil
		for i := int(rf.nextCommitIndex.Load()); i < arg.LeaderCommitIndex; i++ {
			rf.sendMsg(nil, true, rf.entries[i], i+1)()
		}
		rf.nextCommitIndex.Store(int32(arg.LeaderCommitIndex))
	}
	if arg.Entries == nil {
		return
	}
	// Leader发送了一个或多个新的cmd,将其设置为预提交
	rf.readyEntries = make([]interface{}, len(arg.Entries))
	copy(rf.readyEntries, arg.Entries)
}

func (rf *Raft) sendHeartBeat() {
	var loseConnectNum atomic.Int32
	var wg sync.WaitGroup
	fn := func(server int) func() {
		return func() {
			defer wg.Done()
			arg := &AppendEntriesArgs{Term: int(rf.term.Load()), Entries: nil, LeaderCommitIndex: int(rf.nextCommitIndex.Load()), Replenish: false}
			reply := &AppendEntriesReply{Success: false, ExpectEntriesStart: -1}
			if !rf.sendRPC(server, "Raft.AppendEntries", arg, reply, 20*time.Millisecond) {
				loseConnectNum.Add(1)
				return
			}
			if !reply.Success {
				if reply.ExpectEntriesStart != -1 {
					rf.replenishEntries(server, reply.ExpectEntriesStart, nil)
				}
				return
			}
		}
	}

	for rf.isLeader.Load() {
		for i := range rf.peers {
			if i != rf.me {
				wg.Add(1)
				rf.manager.AddTask(RandomQueue, fn(i))
			}
		}
		wg.Wait()
		if int(loseConnectNum.Load()) >= len(rf.peers)-1 { // 如果这个leader除了自己谁都联系不上
			rf.isLeader.Store(false)
			return
		}
		loseConnectNum.Store(0)
		time.Sleep(heartBeatInterval)
	}
}

func (rf *Raft) loseHeart() bool {
	if rf.isLeader.Load() {
		return false
	}
	return time.Now().UnixMilli()-rf.lastHeart.Load() > tolerableHeartBeatInterval.Milliseconds()
}

func (rf *Raft) submitLog(command interface{}) func() {
	sucCount := atomic.Int32{}
	sucCount.Store(1)
	wg := sync.WaitGroup{}
	fn := func(i int) {
		defer wg.Done()
		arg := &AppendEntriesArgs{Term: int(rf.term.Load()), Entries: []interface{}{command}, LeaderCommitIndex: int(rf.nextCommitIndex.Load()), Replenish: false}
		reply := &AppendEntriesReply{Success: false, ExpectEntriesStart: -1}
		if !rf.sendRPC(i, "Raft.AppendEntries", arg, reply, 20*time.Millisecond) {
			rf.retryAppend(i, 3, arg, reply)
			return
		}
		if !reply.Success { // 如果没有成功
			if reply.ExpectEntriesStart != -1 { // 尝试判断是不是因为缺失数据导致的失败，如果是,那就补齐
				rf.replenishEntries(i, reply.ExpectEntriesStart, func() { // 发送缺失的数据
					sucCount.Add(1)
				})
			}
			return
		}
		sucCount.Add(1)
	}

	return func() {
		rf.mu.Lock()
		rf.entries = append(rf.entries, command)
		rf.mu.Unlock()
		for i := range rf.peers {
			if i != rf.me {
				wg.Add(1)
				server := i
				go fn(server)
			}
		}
		wg.Wait()
		if int(sucCount.Load()) <= len(rf.peers)/2 {
			rf.mu.Lock()
			rf.entries = rf.entries[:len(rf.entries)-1]
			rf.mu.Unlock()
			rf.startIndex.Add(-1)
			rf.isLeader.Store(false)               // 失去足够的连接,也保证了entry是一条一条添加的,如果还有节点存活,包括本leader在内,仍然保有这次未完成的entry
			rf.manager.ClearTaskQueue(rf.submitTp) // 取消后面的任务,避免不必要的开销,以及可能导致的歧义
			return
		}
		rf.sendMsg(nil, true, rf.entries[int(rf.nextCommitIndex.Load())], int(rf.nextCommitIndex.Load())+1)()
		rf.nextCommitIndex.Add(1)
	}
}

func (rf *Raft) retryAppend(server int, retryTimes int, arg, reply interface{}) bool {
	for i := 0; i < retryTimes; i++ {
		arg1, reply1 := arg, reply
		if rf.sendRPC(server, "Raft.AppendEntries", arg1, reply1, 10*time.Millisecond) {
			return true
		}
	}
	return false
}

func (rf *Raft) replenishEntries(server int, startIndex int, callback func()) {
	arg := &AppendEntriesArgs{
		Term:              int(rf.term.Load()),
		LeaderCommitIndex: int(rf.nextCommitIndex.Load()),
		Replenish:         true,
	}
	reply := &AppendEntriesReply{}
	rf.mu.Lock()
	arg.Entries = rf.entries[startIndex:len(rf.entries)]
	rf.mu.Unlock()
	if rf.sendRPC(server, "Raft.AppendEntries", arg, reply, 30*time.Millisecond) && reply.Success {
		if callback != nil {
			callback()
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
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
	rf.dead.Store(true)
	rf.isLeader.Store(false)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	return rf.dead.Load()
}

const heartBeatInterval = 150 * time.Millisecond         // 150
const tolerableHeartBeatInterval = 3 * heartBeatInterval // 5

func (rf *Raft) ticker() {
	for rf.killed() == false {
		if !rf.loseHeart() { // 没有超过可容忍时间或者此节点是leader就跳过选举
			time.Sleep(10 * time.Millisecond)
			continue
		}
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
			rf.mu.Lock()
			if !reply.GrantVote || !rf.isCandidate.Load() {
				vote = 0
				rf.term.Store(int32(newterm))
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
			vote++
		}
		if vote > len(rf.peers)/2 {
			rf.isLeader.Store(true) // 竞选成功,下一步发送心跳
			rf.term.Store(int32(newterm))
			rf.startIndex.Store(rf.nextCommitIndex.Load())
			go rf.sendHeartBeat()
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
	rf.dead.Store(false)
	rf.term.Store(0)
	rf.isLeader.Store(false)
	rf.isCandidate.Store(false)
	rf.lastHeart.Store(time.Now().UnixMilli())

	rf.manager = NewTaskManager(10)
	rf.submitTp = rf.manager.NewTaskType()
	rf.entries = make([]interface{}, 0)
	rf.nextCommitIndex.Store(0)
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
