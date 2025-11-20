package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	follower = iota
	candidate
	leader
)

type Log struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	ApplyCh   chan raftapi.ApplyMsg
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm     int
	role            int //0 follower, 1 candidate, 2 leader
	electionTimeout int64
	log             []Log
	commitIndex     int //log index that has been commited
	lastApplied     int //log index that has been applied to state machine
	votedFor        int // who I vote for
	voteList        []int
	LeaderSending   bool //is leader sending AppendEntries to follower
	// AgreeCommitCount []int //list for log index agreement
	nextIndex         []int //log index that leader should send to follower
	matchIndex        []int //max log index that follower has received
	firstCurTermIndex int   //log index of last term,such as   term:1 1 1 2 2 2 3 ,cur term=3,last Term index = 7
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	term = rf.currentTerm

	if rf.role == 2 {
		isleader = true
	} else {
		isleader = false
	}
	// Your code here (3A).
	return term, isleader
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// heartbeat message
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) LastLogTerm() int {
	if len(rf.log) == 0 {
		return -1
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 1. 如果请求中的 term 比自己大，更新自己
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = follower
		rf.votedFor = -1
	}

	// 2. 如果请求的 term 比自己小，拒绝
	if args.Term < rf.currentTerm {
		return
	}

	// 3. 检查是否已经投票 & 日志是否新
	lastTerm := rf.LastLogTerm()
	lastIndex := rf.lastLogIndex()
	upToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.electionTimeout = int64(300 + rand.Intn(300))
	}

	reply.Term = rf.currentTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//rev RPC , update term and role
	rf.electionTimeout = int64(300 + rand.Intn(300))

	//check leader term
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = follower
	} else {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	//follower lack of log
	if args.PrevLogIndex > len(rf.log) {
		reply.Success = false
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		// fmt.Println("follower index < leader log index")
		// fmt.Println("me:", rf.me)
		return
	}

	if args.PrevLogIndex > 0 {
		if rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			reply.Success = false
			reply.ConflictTerm = rf.log[args.PrevLogIndex-1].Term
			reply.ConflictIndex = args.PrevLogIndex

			for i := len(rf.log); i > 0; i-- {
				if rf.log[i-1].Term == reply.ConflictTerm {
					continue
				} else {
					reply.ConflictIndex = i
				}
			}

			// rf.log = rf.log[:rf.commitIndex-1]
			// fmt.Println("me:", rf.me)
			// fmt.Println("follower term != leader log term,need to rollback")
			// fmt.Println("len(log):", len(rf.log))
			return
		}
		//当term对时能更新commitIndex
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
			// fmt.Println("latest commit:", rf.commitIndex)
		}
	}

	// fmt.Println(len(args.Entries))
	//log Replication
	if len(args.Entries) > 0 {

		//要保证prevlogindex后面是空的,例如 follower:1:10,2:20 leader:1:10,2:20,应该删除follower的2:20

		rf.log = append(rf.log[:args.PrevLogIndex], args.Entries...)

		// fmt.Println("me:", rf.me)
		// fmt.Println("now my log len :", len(rf.log))

		// fmt.Println("now me:", rf.me, "loglen:", len(rf.log))
	}

	reply.Success = true

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// leader sendAppendEntries means that the leader use args to call other servers' AppendEntries RPC
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendLog(command interface{}) (int, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//append log
	rf.log = append(rf.log, Log{rf.currentTerm, command})
	//commit itself
	return len(rf.log), rf.currentTerm
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
	term := -1
	isLeader := true

	// Your code here (3B).

	if rf.role != leader {
		isLeader = false
		return index, term, isLeader
	}

	index, term = rf.AppendLog(command)

	// go rf.WaitForCommit(index)
	// fmt.Println("server:", rf.me)
	// fmt.Println("start return index , term ,is lerader:", index, term, isLeader)
	return index, term, isLeader
}

// todo
// func (rf *Raft) WaitForCommit(index int) {

// 	for !rf.killed() {
// 		// print("wait for follower commit\n")

// 		rf.mu.Lock()

// 		if rf.role != leader {
// 			rf.mu.Unlock()
// 			return
// 		}

// 		AgreeCommitCount := 1
// 		for i := 0; i < len(rf.matchIndex); i++ {
// 			if rf.matchIndex[i] >= index {
// 				AgreeCommitCount++
// 			}
// 		}

// 		if AgreeCommitCount > len(rf.peers)/2 {
// 			// print("success commit")
// 			rf.commitIndex = index
// 			rf.mu.Unlock()
// 			return
// 		}
// 		rf.mu.Unlock()

// 		time.Sleep(time.Millisecond * 10)
// 	}

// }

// TO DO  commit 的并发问题，或者其他的
func (rf *Raft) UpdateCommitIndex() {

	// print("wait for follower commit\n")

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.commitIndex = rf.lastApplied
	for N := rf.commitIndex + 1; N <= len(rf.log); N++ {
		AgreeCommitCount := 1
		for i := 0; i < len(rf.matchIndex); i++ {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= N {
				AgreeCommitCount++
			}
		}

		if AgreeCommitCount > len(rf.peers)/2 && rf.log[N-1].Term == rf.currentTerm {
			// print("success commit")
			rf.commitIndex = N
			// fmt.Println("leader commit:", rf.me, "  ", rf.commitIndex)

		} else {
			break
		}

	}
	// fmt.Println("server:", rf.me)
	// fmt.Println("my commit index:", rf.commitIndex)
}

func (rf *Raft) ApplyCommit() {
	for !rf.killed() {
		time.Sleep(time.Millisecond * 100)

		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied && len(rf.log) > rf.lastApplied && len(rf.log) != 0 {
			entry := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied + 1,
			}
			//apply chan cant lock
			rf.ApplyCh <- entry
			// fmt.Println("server apply:", rf.me, "  ", entry.CommandIndex)
			rf.lastApplied++
		}
		rf.mu.Unlock()

	}
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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) Atomic_ChangeRoleTO(role int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = role
}

func (rf *Raft) CleanVoteList() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := 0; i < len(rf.voteList); i++ {
		rf.voteList[i] = 0
	}
}

func (rf *Raft) getPrevLogindex_term(peer int) (int, int) {
	PrevLogIndex := rf.nextIndex[peer] - 1
	PrevLogTerm := 0
	if PrevLogIndex <= 0 {
		PrevLogIndex = 0
		PrevLogTerm = 0
	} else {
		PrevLogTerm = rf.log[PrevLogIndex-1].Term
	}
	return PrevLogIndex, PrevLogTerm
}

func (rf *Raft) Replication(peer int) {

	for !rf.killed() {
		time.Sleep(time.Millisecond * 30)

		rf.mu.Lock()
		if rf.role != leader {
			rf.mu.Unlock()
			return
		}

		AppendEntriesArgs := AppendEntriesArgs{rf.currentTerm, rf.me, 0, 0, nil, rf.commitIndex}
		//send appendEntries with log
		AppendEntriesReply := AppendEntriesReply{}
		//to make sure log copy is successful
		PrevLogIndex, PrevLogTerm := rf.getPrevLogindex_term(peer)
		AppendEntriesArgs.PrevLogIndex = PrevLogIndex
		AppendEntriesArgs.PrevLogTerm = PrevLogTerm

		if rf.nextIndex[peer] <= len(rf.log) && rf.nextIndex[peer] > 0 {
			entries := make([]Log, len(rf.log[rf.nextIndex[peer]-1:]))
			copy(entries, rf.log[rf.nextIndex[peer]-1:])
			AppendEntriesArgs.Entries = entries
		} else {
			AppendEntriesArgs.Entries = nil
		}
		rf.mu.Unlock()

		// fmt.Println("send log[last].term", AppendEntriesArgs.PrevLogTerm)
		// fmt.Println("send log[last]", AppendEntriesArgs.Entries)
		// AppendEntriesArgs.Entries = rf.log[AppendEntriesArgs.PrevLogIndex-1:]

		ok := rf.sendAppendEntries(peer, &AppendEntriesArgs, &AppendEntriesReply)
		if ok {

			if AppendEntriesReply.Success {
				rf.mu.Lock()
				rf.nextIndex[peer] = AppendEntriesArgs.PrevLogIndex + len(AppendEntriesArgs.Entries) + 1
				rf.matchIndex[peer] = rf.nextIndex[peer] - 1

				rf.mu.Unlock()

				rf.UpdateCommitIndex()

				continue
			}
			if AppendEntriesReply.Term > rf.currentTerm {
				rf.mu.Lock()
				rf.role = follower
				rf.currentTerm = AppendEntriesReply.Term
				rf.mu.Unlock()
				continue
			}

			rf.mu.Lock()
			if AppendEntriesReply.ConflictTerm == -1 {
				//leader log index > follower log index , example: leader log index = 5, follower log index = 1
				//so AppendEntriesReply.ConflictIndex = 1,nextIndex = 1+1=2
				rf.nextIndex[peer] = AppendEntriesReply.ConflictIndex + 1
			} else {
				//leader log index <= follower log index , example: leader log index = 5, follower log index = 5
				//but follower's log[5] term is not equal to leader log[5] term
				//so AppendEntriesReply.ConflictIndex = 5,nextIndex = 5-1+1=5(6->5)
				rf.nextIndex[peer] = AppendEntriesReply.ConflictIndex
			}
			rf.mu.Unlock()

		}
	}

}

func (rf *Raft) ReplicationLoop() {
	// fmt.Println("now leader is :", rf.me)
	// fmt.Println("now leader term is :", rf.currentTerm)
	// fmt.Println("leader commit index and log len", rf.commitIndex, len(rf.log))
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.Replication(i)
		}
	}

}

func (rf *Raft) getVoteState() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		if rf.role == follower {
			rf.mu.Lock()
			for i := 0; i < len(rf.voteList); i++ {
				rf.voteList[i] = 0
			}
			rf.mu.Unlock()
			return
		}

		rf.mu.Lock()
		rf.electionTimeout -= 10
		if rf.voteList[rf.me] > len(rf.peers)/2 {

			//clean voteList
			rf.role = leader
			for i := 0; i < len(rf.voteList); i++ {
				rf.voteList[i] = 0
			}
			//reset nextIndex and matchIndex
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = rf.lastApplied + 1
				rf.matchIndex[i] = 0
			}
			rf.mu.Unlock()

			go rf.ReplicationLoop()
			go rf.leader()
			return
		}
		rf.mu.Unlock()

	}
}
func (rf *Raft) startElection() {
	rf.mu.Lock()

	DPrintf("server %d start election", rf.me)
	rf.currentTerm++
	for i := 0; i < len(rf.voteList); i++ {
		rf.voteList[i] = 0
	}

	//vote myself
	rf.votedFor = rf.me
	rf.voteList[rf.me] = 1
	rf.mu.Unlock()

	//send voteRequest to all followers
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(peer int) {
				RequestVoteArgs := RequestVoteArgs{rf.currentTerm, rf.me, len(rf.log), 0}
				if len(rf.log) > 0 {
					RequestVoteArgs.LastLogTerm = rf.log[len(rf.log)-1].Term
				}
				RequestVoteReply := RequestVoteReply{}
				ok := rf.sendRequestVote(peer, &RequestVoteArgs, &RequestVoteReply)
				if ok {
					if RequestVoteReply.VoteGranted {
						rf.mu.Lock()
						rf.voteList[rf.me]++
						rf.mu.Unlock()
					}
				}
			}(i)

		}
	}

	//get vote state
	rf.getVoteState()

}

func (rf *Raft) heartbeat(peer int) {
	for !rf.killed() {
		time.Sleep(time.Millisecond * 100)

		rf.mu.Lock()
		if rf.role != leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		rf.mu.Lock()
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: -1,
			PrevLogTerm:  -1,
			Entries:      []Log{},
			LeaderCommit: rf.commitIndex,
		}
		args.PrevLogIndex, args.PrevLogTerm = rf.getPrevLogindex_term(peer)
		rf.mu.Unlock()

		// AppendEntriesArgs.Entries = rf.log[AppendEntriesArgs.PrevLogIndex-1:]
		AppendEntriesReply := AppendEntriesReply{}

		ok := rf.sendAppendEntries(peer, &args, &AppendEntriesReply)
		if ok {
			if AppendEntriesReply.Success {
				continue
			}
			if AppendEntriesReply.Term > rf.currentTerm {
				rf.mu.Lock()
				rf.role = follower
				rf.currentTerm = AppendEntriesReply.Term
				rf.mu.Unlock()
				continue
			}
		}

	}
}

func (rf *Raft) leader() {
	DPrintf("server %d is leader,sending heartbeat", rf.me)

	//TODO,100ms send a heartbeat to all followers

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.heartbeat(i)
		}
	}

}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		if rf.role == follower {
			//wg to wait for all vote request to finish
			if rf.electionTimeout <= 0 {
				DPrintf("election timeout\n")
				rf.Atomic_ChangeRoleTO(candidate)
			} else {
				rf.electionTimeout -= ms
			}
		}

		if rf.role == candidate {

			//TODO,start a leader election
			rf.startElection()
		}

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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.electionTimeout = int64(300 + rand.Intn(300))
	rf.votedFor = -1
	rf.voteList = make([]int, len(peers))
	rf.role = follower
	rf.LeaderSending = false
	rf.log = []Log{}
	rf.lastApplied = 0
	rf.commitIndex = 0

	rf.ApplyCh = applyCh

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = len(rf.log) + 1
		rf.matchIndex[i] = 0
	}

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.ApplyCommit()

	return rf
}
