package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"fmt"
	"math/rand"
	"strconv"
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
	currentTerm      int
	role             int //0 follower, 1 candidate, 2 leader
	electionTimeout  int64
	log              []Log
	commitIndex      int //log index that has been commited
	lastApplied      int //log index that has been applied to state machine
	votedFor         int // who I vote for
	voteList         []int
	LeaderSending    bool //is leader sending AppendEntries to follower
	AgreeCommitCount int
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
	LastLogterm  int
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
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			if args.LastLogIndex >= rf.commitIndex {
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
			}
		}

		//if revice a bigger term, become follower and reset timeout
		rf.electionTimeout = int64(500 + rand.Intn(400))
		rf.currentTerm = args.Term
		rf.role = follower //role change to follower
		reply.VoteGranted = true
		return
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term >= rf.currentTerm {
		//if revice a bigger term, become follower and reset timeout
		rf.electionTimeout = int64(500 + rand.Intn(400))
		rf.currentTerm = args.Term
		rf.role = follower //role change to follower

		//update log,frist check log index and term

		if len(args.Entries) != 0 {
			print(args.PrevLogTerm)
			print(len(rf.log))
			print("\n")
			if args.PrevLogIndex == len(rf.log) {
				if len(rf.log) > 0 && rf.log[len(rf.log)-1].Term != args.PrevLogTerm {
					print(args.PrevLogTerm)
					print(rf.log[len(rf.log)-1].Term)

					print(" term is not match \n")
					DPrintf("log index or term is not match \n")
					reply.Success = false
					reply.Term = rf.currentTerm
					return
				}
				print("Leadercommit :" + strconv.Itoa(args.LeaderCommit))
				if args.LeaderCommit >= rf.commitIndex {
					go func() {
						for i := rf.commitIndex + 1; i <= args.LeaderCommit; i++ {
							//TODO DEBUG TEST3B-2 ，FINISH THIS TEST3B-1
							print("commit index:" + strconv.Itoa(i) + " ")
							rf.ApplyCh <- raftapi.ApplyMsg{
								CommandValid: true,
								Command:      rf.log[i].Command,
								CommandIndex: i,
							}
						}
						rf.lastApplied = args.LeaderCommit
					}()

					rf.commitIndex = args.LeaderCommit

					rf.log = append(rf.log, args.Entries...)
					print(rf.me)
					print("now log len:" + strconv.Itoa(len(rf.log)) + " ")
					fmt.Println("cmd:", rf.log[len(rf.log)-1])
					print("finish commit once \n")

				} else {
					print("follower commit index is bigger , to be done \n")
					DPrintf("follower commit index is bigger , to be done \n")
				}
			} else {
				print("log index is not match \n")
				DPrintf("log index is not match \n")
				//reply.Success = false

			}
		}

		reply.Success = true

	} else {
		reply.Success = false
		reply.Term = rf.currentTerm
	}
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
	rf.log = append(rf.log, Log{rf.currentTerm, command})
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

	PrevLogIndex := len(rf.log)
	PrevLogTerm := 0
	if len(rf.log) > 0 {
		PrevLogTerm = rf.log[len(rf.log)-1].Term
	}

	index, term = rf.AppendLog(command)
	rf.mu.Lock()
	rf.AgreeCommitCount = 1
	rf.mu.Unlock()
	AppenLog := []Log{}
	AppenLog = append(AppenLog, Log{rf.currentTerm, command})

	AppendEntriesArgs := AppendEntriesArgs{rf.currentTerm, rf.me, PrevLogIndex, PrevLogTerm, AppenLog, rf.commitIndex}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(peer int) {
				AppendEntriesReply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, &AppendEntriesArgs, &AppendEntriesReply)
				if ok {

					if !AppendEntriesReply.Success {
						rf.Atomic_ChangeRoleTO(follower)
						rf.currentTerm = AppendEntriesReply.Term
					} else {
						rf.mu.Lock()
						rf.AgreeCommitCount++
						rf.mu.Unlock()
					}
				}

			}(i)

		}
	}

	for {
		if rf.killed() {
			return index, term, isLeader
		}
		if rf.AgreeCommitCount > len(rf.peers)/2 {
			rf.mu.Lock()
			print("success")

			for i := rf.commitIndex + 1; i <= index; i++ {
				print("commit succ")
				rf.ApplyCh <- raftapi.ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i].Command,
					CommandIndex: i,
				}
			}
			rf.commitIndex = index
			rf.AgreeCommitCount = 0
			rf.mu.Unlock()
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	return index, term, isLeader
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

func (rf *Raft) startElection() {
	rf.mu.Lock()

	DPrintf("server %d start election", rf.me)
	rf.currentTerm++
	for i := 0; i < len(rf.voteList); i++ {
		rf.voteList[i] = 0
	}

	//vote myself
	rf.votedFor = rf.me
	rf.voteList[rf.me]++
	//send voteRequest to all followers
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(peer int) {
				RequestVoteReply := RequestVoteReply{}
				ok := rf.sendRequestVote(peer, &RequestVoteArgs{rf.currentTerm, rf.me, rf.commitIndex, rf.lastApplied}, &RequestVoteReply)
				if ok {
					if RequestVoteReply.VoteGranted {
						rf.voteList[rf.me]++
					}
				}
			}(i)

		}
	}

	rf.mu.Unlock()

	//get vote state
	go func() {
		for {
			if rf.role == follower {
				rf.mu.Lock()
				for i := 0; i < len(rf.voteList); i++ {
					rf.voteList[i] = 0
				}
				rf.mu.Unlock()
				return
			}
			// DPrintf("server" + strconv.Itoa(rf.me%len(rf.peers)) + " voteList[me]:" + strconv.Itoa(rf.voteList[rf.me]))
			if rf.voteList[rf.me] > len(rf.peers)/2 {
				rf.mu.Lock()
				rf.role = leader
				for i := 0; i < len(rf.voteList); i++ {
					rf.voteList[i] = 0
				}
				rf.mu.Unlock()
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

}

func (rf *Raft) leader() {
	DPrintf("server %d is leader,sending heartbeat", rf.me)
	for rf.role == leader {
		rf.LeaderSending = true
		//TODO,100ms send a heartbeat to all followers
		AppendEntriesArgs := AppendEntriesArgs{}
		AppendEntriesArgs.Term = rf.currentTerm
		AppendEntriesArgs.LeaderId = rf.me
		AppendEntriesArgs.PrevLogIndex = rf.commitIndex
		AppendEntriesArgs.PrevLogTerm = 0
		AppendEntriesArgs.Entries = []Log{}
		AppendEntriesArgs.LeaderCommit = rf.commitIndex

		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(peer int) {
					AppendEntriesReply := AppendEntriesReply{}
					ok := rf.sendAppendEntries(i, &AppendEntriesArgs, &AppendEntriesReply)
					if ok {

						if !AppendEntriesReply.Success {
							rf.Atomic_ChangeRoleTO(follower)
							rf.currentTerm = AppendEntriesReply.Term
						}
					}

				}(i)

			}
		}

		time.Sleep(100 * time.Millisecond)

	}
	rf.LeaderSending = false

}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		DPrintf("tick\n")
		if rf.role == follower {
			//wg to wait for all vote request to finish
			DPrintf("server" + strconv.Itoa(rf.me) + "is follower")
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

		if rf.role == leader {
			if rf.LeaderSending {
				continue
			}
			go rf.leader()
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
	rf.AgreeCommitCount = 0

	rf.ApplyCh = applyCh
	// go func() {
	// 	//TODO use chan pass msg to applyCh
	// 	for i := 1; i <= 3; i++ {
	// 		// applyCh <- raftapi.ApplyMsg{
	// 		// 	CommandValid: false,
	// 		// }
	// 		rf.ApplyCh <- raftapi.ApplyMsg{
	// 			CommandValid: true,
	// 			Command:      666,
	// 			CommandIndex: i,
	// 		}

	// 	}
	// 	time.Sleep(time.Millisecond * 100)
	// }()

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
