package raft

import (
	"sync"
	"sync/atomic"
	"time"
)

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
	rf.mu.Lock() // 避免同时处理多个AppendEntries
	rf.term.Store(int32(arg.Term))
	rf.lastHeart.Store(time.Now().UnixMilli())
	if rf.isLeader.Load() { // 如果我是leader,但还是接收到了请求，那说明还存在其他Leader
		if rf.nextCommitIndex < arg.LeaderCommitIndex {
			rf.isLeader.Store(false)
		}
	}
	reply.Success = true
	reply.ExpectEntriesStart = 12345
	if arg.LeaderCommitIndex > rf.nextCommitIndex && !arg.Replenish { // 如果arg的index更大，那说明需要commit了
		if rf.readyEntriesTerm != arg.Term { // 如果换代了
			rf.readyEntries = nil
			rf.readyEntriesTerm = arg.Term
		}
		if len(rf.entries)+len(rf.readyEntries) < arg.LeaderCommitIndex { // 如果commit的entries和准备提交的entries加起来数目对不上，那就说明缺失了一部分
			reply.Success = false
			reply.ExpectEntriesStart = len(rf.entries)
			rf.mu.Unlock()
			return
		}
		// 如果没有缺少数据的话，就commit
		rf.entries = append(rf.entries, rf.readyEntries...)
		DPrintf("Follower: %d commit commit commit entries: %v", rf.me, rf.readyEntries)
		rf.readyEntries = nil
		commitIndex := rf.nextCommitIndex
		rf.nextCommitIndex = arg.LeaderCommitIndex
		rf.persist()
		for i := commitIndex; i < rf.nextCommitIndex; i++ {
			rf.sendMsg(nil, true, rf.entries[i], i+1)()
		}
	}
	if arg.Entries == nil {
		rf.mu.Unlock()
		return
	}
	// Leader发送了一个或多个新的cmd,将其设置为预提交
	rf.readyEntries = make([]interface{}, len(arg.Entries))
	copy(rf.readyEntries, arg.Entries)
	DPrintf("Follower: %d received entries: %v", rf.me, rf.readyEntries)
	rf.readyEntriesTerm = arg.Term
	rf.mu.Unlock()
}

func (rf *Raft) submitLog(command interface{}) func() {
	sucCount := atomic.Int32{}
	sucCount.Store(1)
	wg := sync.WaitGroup{}
	fn := func(i int) {
		defer wg.Done()
		arg := &AppendEntriesArgs{Term: int(rf.term.Load()), Entries: []interface{}{command}, LeaderCommitIndex: rf.nextCommitIndex, Replenish: false}
		reply := &AppendEntriesReply{Success: true, ExpectEntriesStart: -1}
		if !rf.sendRPC(i, "Raft.AppendEntries", arg, reply, 20*time.Millisecond) {
			rf.retryAppend(i, 3, []interface{}{command})
			return
		}

		if !reply.Success { // 如果没有成功
			if reply.ExpectEntriesStart != -1 { // 尝试判断是不是因为缺失数据导致的失败，如果是,那就补齐
				rf.replenishEntries(i, reply.ExpectEntriesStart, func() { // 发送缺失的数据
					rf.sucMap[i] = true
					sucCount.Add(1)
				})
			}
			return
		}
		rf.mu.Lock()
		rf.sucMap[i] = true
		rf.mu.Unlock()
		sucCount.Add(1)
	}

	return func() {
		if !rf.isLeader.Load() {
			return
		}
		rf.mu.Lock()
		rf.entries = append(rf.entries, command)
		rf.mu.Unlock()
		if len(rf.sucMap) != len(rf.peers) {
			rf.sucMap = make(map[int]bool, len(rf.peers))
		}
		for i := range rf.peers {
			if i != rf.me {
				wg.Add(1)
				go fn(i)
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
			rf.persist()
			return
		}
		if rf.killed() {
			return
		}
		rf.nextCommitIndex++
		rf.lastHeart.Store(time.Now().UnixMilli())
		for i := range rf.sucMap {
			if i != rf.me && rf.sucMap[i] {
				arg := &AppendEntriesArgs{Term: int(rf.term.Load()), Entries: nil, LeaderCommitIndex: rf.nextCommitIndex, Replenish: false}
				reply := &AppendEntriesReply{Success: true, ExpectEntriesStart: -1}
				rf.sendRPC(i, "Raft.AppendEntries", arg, reply)
			}
			rf.sucMap[i] = false
		}
		rf.persist()
		rf.sendMsg(nil, true, rf.entries[rf.nextCommitIndex-1], rf.nextCommitIndex)()
		DPrintf("Leader: %d commit a command: %v", rf.me, command)
	}
}

func (rf *Raft) retryAppend(server int, retryTimes int, entries []interface{}) bool {
	for i := 0; i < retryTimes; i++ {
		arg1, reply1 := &AppendEntriesArgs{
			Term:              int(rf.term.Load()),
			Entries:           entries,
			LeaderCommitIndex: rf.nextCommitIndex,
			Replenish:         false,
		}, &AppendEntriesReply{
			Success:            false,
			ExpectEntriesStart: -1,
		}
		if rf.sendRPC(server, "Raft.AppendEntries", arg1, reply1, 10*time.Millisecond) {
			return true
		}
	}
	return false
}

func (rf *Raft) replenishEntries(server int, startIndex int, callback func()) {
	arg := &AppendEntriesArgs{
		Term:              int(rf.term.Load()),
		LeaderCommitIndex: rf.nextCommitIndex,
		Replenish:         true,
	}
	reply := &AppendEntriesReply{Success: false, ExpectEntriesStart: -1}
	rf.mu.Lock()
	arg.Entries = rf.entries[startIndex:len(rf.entries)]
	rf.mu.Unlock()

	if rf.sendRPC(server, "Raft.AppendEntries", arg, reply, 30*time.Millisecond) {
		if reply.Success && callback != nil {
			callback()
		}
	}
}
