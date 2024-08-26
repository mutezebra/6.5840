package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type RequestVoteArgs struct {
	Term        int
	CandidateID int
	CommitIndex int
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
	if rf.nextCommitIndex > args.CommitIndex {
		reply.GrantVote = false
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.isCandidate.Load() {
		if term < args.Term-1 {
			rf.isCandidate.Store(false)
			DPrintf("Candidate: %d 愿意给%d投票,term: %d,nCI: %d. arg.Term: %d,arg.nCI: %d.Time: %d", rf.me, args.CandidateID, term, rf.nextCommitIndex, args.Term, args.CommitIndex, time.Now().UnixNano())
			vote()
			return
		}
		reply.GrantVote = false // term == args.Term
		return
	}
	DPrintf("Follower: %d 愿意给%d投票,term: %d,nCI: %d. arg.Term: %d,arg.nCI: %d.Time: %d", rf.me, args.CandidateID, term, rf.nextCommitIndex, args.Term, args.CommitIndex, time.Now().UnixNano())
	vote()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.sendRPC(server, "Raft.RequestVote", args, reply)
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		if !rf.loseHeart() { // 没有超过可容忍时间或者此节点是leader就跳过选举
			time.Sleep(10 * time.Millisecond)
			continue
		}
		rf.mu.Lock()
		rf.isCandidate.Store(true)
		DPrintf("Follower: %d 成为了Candidate.Time: %d", rf.me, time.Now().UnixNano())
		rf.mu.Unlock()
		newterm := int(rf.term.Load() + 1)
		var vote int
		for i := range rf.peers {
			if i == rf.me {
				vote++
				continue
			}
			arg := &RequestVoteArgs{Term: newterm, CandidateID: rf.me, CommitIndex: rf.nextCommitIndex}
			reply := &RequestVoteReply{}
			if ok := rf.sendRequestVote(i, arg, reply); !ok {
				continue
			}
			rf.mu.Lock()
			if !reply.GrantVote {
				vote = 0
				rf.mu.Unlock()
				break
			}
			if !rf.isCandidate.Load() {
				vote = 0
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
			vote++
		}
		if vote > len(rf.peers)/2 {
			rf.isLeader.Store(true) // 竞选成功,下一步发送心跳
			rf.term.Store(int32(newterm))
			rf.startIndex.Store(int32(rf.nextCommitIndex))
			DPrintf("Leader: %d 成为了Leader", rf.me)
			go rf.sendHeartBeat()
		}
		rf.isCandidate.Store(false)
		ms := 150 + (rand.Int63() % 250)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) sendHeartBeat() {
	var loseConnectNum atomic.Int32
	var wg sync.WaitGroup
	fn := func(server int) func() {
		return func() {
			defer wg.Done()
			arg := &AppendEntriesArgs{Term: int(rf.term.Load()), Entries: nil, LeaderCommitIndex: rf.nextCommitIndex, Replenish: false}
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
		if int(loseConnectNum.Load()) >= len(rf.peers)-1 { // 如果这个leader除了自己谁都联系不上，说明是自己掉线了
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
