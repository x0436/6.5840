package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type LogEntry struct {
	Command interface{}
	Term    int
}

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyCh chan raftapi.ApplyMsg

	lastHeartbeatTime time.Time

	state NodeState
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == Leader
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

func (rf *Raft) electionTimeout() time.Duration {
	return time.Duration(300+rand.Int63()%300) * time.Millisecond
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.state = Candidate
	rf.currentTerm++
	term := rf.currentTerm
	rf.votedFor = rf.me
	rf.lastHeartbeatTime = time.Now()

	lastIndex, lastTerm := rf.lastLogInfo()

	args := &RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	rf.persist()
	rf.mu.Unlock()

	votes := 1
	if votes > len(rf.peers)/2 {
		rf.becomeLeaderLocked()
		go rf.sendHeartbeats()
		return
	}

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(peer int) {
			reply := &RequestVoteReply{}

			if !rf.sendRequestVote(peer, args, reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.persist()
				return
			}

			// RPC 返回时，本次选举可能已经过期。
			if rf.state != Candidate || rf.currentTerm != term {
				return
			}

			if reply.VoteGranted {
				votes++

				if votes > len(rf.peers)/2 {
					rf.becomeLeaderLocked()
					go rf.sendHeartbeats()
				}
			}
		}(peer)
	}
}

func (rf *Raft) becomeLeaderLocked() {
	rf.state = Leader

	next := len(rf.log)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		rf.nextIndex[i] = next
		rf.matchIndex[i] = -1
	}

	rf.lastHeartbeatTime = time.Now()
}

func (rf *Raft) lastLogInfo() (int, int) {
	lastIndex := len(rf.log) - 1
	lastTerm := 0
	if lastIndex >= 0 {
		lastTerm = rf.log[lastIndex].Term
	}
	return lastIndex, lastTerm
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}

	reply.Term = rf.currentTerm

	lastIndex, lastTerm := rf.lastLogInfo()
	candidateUpToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateId

	if canVote && candidateUpToDate {
		rf.votedFor = args.CandidateId
		rf.state = Follower
		rf.lastHeartbeatTime = time.Now()
		rf.persist()

		reply.VoteGranted = true
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendHeartbeat(server int) {
	args := rf.makeAppendEntriesArgs(server)
	args.Entries = []LogEntry{}

	rf.sendAppendEntries(server, args)
}

func (rf *Raft) sendLogEntries(server int) {
	args := rf.makeAppendEntriesArgs(server)
	rf.sendAppendEntries(server, args)
}

func (rf *Raft) makeAppendEntriesArgs(server int) *AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := 0
	if prevLogIndex >= 0 {
		prevLogTerm = rf.log[prevLogIndex].Term
	}

	entries := append([]LogEntry(nil), rf.log[rf.nextIndex[server]:]...)

	return &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
		return
	}

	if reply.Success {
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	} else if rf.nextIndex[server] > 1 {
		rf.nextIndex[server]--
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	rf.state = Follower
	rf.lastHeartbeatTime = time.Now()
	reply.Term = rf.currentTerm
	// 检查 PrevLogIndex / PrevLogTerm
	// 冲突时删除并追加 args.Entries
	// 根据 LeaderCommit 更新 commitIndex

	reply.Success = true
}

func (rf *Raft) sendHeartbeats() {
	for peer := range rf.peers {
		if peer != rf.me {
			go rf.sendHeartbeat(peer)
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader { // 非leader
		return -1, rf.currentTerm, false
	}

	index := len(rf.log)
	term := rf.currentTerm

	rf.log = append(rf.log, LogEntry{ // 将新的命令加入到 log里
		Command: command,
		Term:    term,
	})
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = len(rf.log)
	rf.persist() // 为什么在这里持久化

	for peer := range rf.peers { // 遍历peers, 将日志条目发送给followers
		if peer != rf.me {
			go rf.sendLogEntries(peer)
		}
	}

	return index, term, true
}

func (rf *Raft) ticker() {
	electionTimeout := rf.electionTimeout()
	heartbeatInterval := 100 * time.Millisecond

	for {
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
		elapsed := time.Since(rf.lastHeartbeatTime)
		state := rf.state
		rf.mu.Unlock()

		if state == Leader {
			if elapsed >= heartbeatInterval {
				rf.mu.Lock()
				if rf.state == Leader {
					rf.lastHeartbeatTime = time.Now()
				}
				rf.mu.Unlock()

				rf.sendHeartbeats()
			}
			continue
		}

		if elapsed >= electionTimeout {
			rf.startElection()
			electionTimeout = rf.electionTimeout()
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
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{Term: 0}}
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.state = Follower
	rf.lastHeartbeatTime = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
