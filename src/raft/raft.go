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
//

import (
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

type ServerState string

const (
	FOLLOWER  ServerState = "FOLLOWER"
	CANDIDATE ServerState = "CANDIDATE"
	LEADER    ServerState = "LEADER"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int // maybe change to lastTerm which only stores the term of last log entry
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	serverState ServerState
	currentTerm int
	votedFor    int
	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	timer       *time.Timer
	totalVotes  int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) saveFollowerState(term int, voteFor int) {
	rf.currentTerm = term
	rf.serverState = FOLLOWER
	rf.votedFor = voteFor
	rf.persist()
}

func (rf *Raft) saveCandidateState() {
	rf.serverState = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
}

func (rf *Raft) saveLeaderState() {
	rf.serverState = LEADER
}

func (rf *Raft) runAsFollower() {
	DPrintf("Server %d running as follower", rf.me)
	rf.timer.Reset(time.Duration(genRandomTimeDuration()) * time.Millisecond)
	select {
	case <-rf.timer.C: // time elapsed, convert to candidate
		DPrintf("Server %d as Follower time elapsed", rf.me)
		rf.mu.Lock()
		rf.saveCandidateState()
		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) runAsCandidate() {
	DPrintf("Server %d running as candidate", rf.me)
	rf.timer.Reset(time.Duration(genRandomTimeDuration()) * time.Millisecond)
	// start election
	go func() {
		rf.mu.Lock()
		// check if heartbeat is received and need to convert to follower before sending requestVote request
		if rf.serverState != CANDIDATE {
			rf.mu.Unlock()
			return
		}
		lastIdx := len(rf.log) - 1
		lastTerm := 0
		if lastIdx != -1 {
			lastTerm = rf.log[lastIdx].Term
		}
		args := RequestVoteArgs{rf.currentTerm, rf.me, lastIdx, lastTerm}
		DPrintf("[Term %d] Server %d sending requestVote", rf.currentTerm, rf.me)
		rf.mu.Unlock()
		for peerID := range rf.peers {
			go func(peerID int) {
				if peerID == rf.me {
					return
				}
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(peerID, &args, &reply)

				if !ok {
					DPrintf("Server %d Failed sending RequestVote to %d", rf.me, peerID)
					return
				}
				rf.mu.Lock()
				DPrintf("[Term %d] Server %d sent requestVote to Server %d, voteGranted = %t", rf.currentTerm, rf.me, peerID, reply.VoteGranted)
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.saveFollowerState(rf.currentTerm, -1)
					rf.mu.Unlock()
					return
				}
				// in case of 2 requestVote go routines races and one of them got reply.Term > rf.currentTerm
				// then rf.currentTerm will be updated and serverState will be FOLLOWER
				if rf.currentTerm != args.Term || rf.serverState != CANDIDATE {
					rf.mu.Unlock()
					return
				}
				// check state in case it has already been converted to leader
				if reply.VoteGranted {
					rf.totalVotes++
					if rf.totalVotes > len(rf.peers)/2 && rf.serverState == CANDIDATE {
						rf.saveLeaderState()
					}
				}
				rf.mu.Unlock()
			}(peerID)
		}
	}()
	select {
	case <-rf.timer.C:
		DPrintf("Server %d as Candidate time elapsed", rf.me)
		rf.mu.Lock()
		rf.saveCandidateState()
		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) runAsLeader() {
	DPrintf("Server %d running as leader", rf.me)
	for peerID := range rf.peers {
		DPrintf("Server %d sending heartbeat to Server %d", rf.me, peerID)
		go func(peerID int) {
			if peerID == rf.me {
				return
			}
			// TODO: populate arg fields for actual log update, currently only for heartbeat
			rf.mu.Lock()
			defer rf.mu.Unlock()
			args, reply := AppendEntriesArgs{Term: rf.currentTerm, LeaderID: rf.me, PrevLogIndex: 0}, AppendEntriesReply{}
			// rf.mu.Unlock()
			ok := rf.sendAppendEntries(peerID, &args, &reply)
			if !ok {
				DPrintf("Server %d failed sending appendEntries to Server %d", rf.me, peerID)
				return
			}
			DPrintf("Server %d got heartbeat reply, reply.Term = %d, currentTerm = %d", rf.me, reply.Term, rf.currentTerm)
			if reply.Term > rf.currentTerm {
				DPrintf("Server %d as leader is stale, in %d", rf.me, peerID)
				rf.currentTerm = reply.Term
				rf.saveFollowerState(rf.currentTerm, -1)
			}
		}(peerID)
		DPrintf("Server %d returned from goroutine heartbeat to Server %d", rf.me, peerID)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	DPrintf("Get State on Server %d, state = %s", rf.me, rf.serverState)
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.serverState == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int // leader's commit index
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// reset election timeout upon receive

	rf.mu.Lock()
	DPrintf("Server %d [Term %d] received heartbeat from Server %d [Term %d]", rf.me, rf.currentTerm, args.LeaderID, args.Term)
	if args.Term < rf.currentTerm {
		// requestor is stale, no need to reset timer
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	rf.timer.Reset(time.Duration(genRandomTimeDuration()) * time.Millisecond)
	rf.votedFor = args.LeaderID
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	DPrintf("Server %d [Term = %d] received vote request from %d [Term = %d]", rf.me, rf.currentTerm, args.CandidateID, args.Term)
	if args.Term < rf.currentTerm { // requestor is stale
		log.Println("Requestor is stale")
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		log.Println("Already voted for others in the same term")
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.saveFollowerState(args.Term, rf.votedFor)
		DPrintf("Server %d is stale and received request vote, converting to follower", rf.me)
	}
	lastIdx := len(rf.log) - 1
	lastTerm := 0
	if lastIdx != -1 {
		lastTerm = rf.log[lastIdx].Term
	}

	if args.LastLogTerm < lastTerm || args.LastLogTerm == lastTerm && args.LastLogIndex < lastIdx {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.persist()
	}

	DPrintf("[Term %d] Server %d voted for Server %d: %t", rf.currentTerm, rf.me, args.CandidateID, reply.VoteGranted)
	rf.mu.Unlock()
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func genRandomTimeDuration() int {
	return rand.Intn(150) + 300
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	DPrintf("Make Server %d", rf.me)
	// Your initialization code here (2A, 2B, 2C).
	rf.log = make([]LogEntry, 0)
	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)
	rf.totalVotes = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.timer = time.NewTimer(time.Duration(genRandomTimeDuration()) * time.Millisecond)
	rf.mu.Lock()
	DPrintf("Server %d started with state %s", rf.me, rf.serverState)
	if rf.serverState == "" {
		rf.saveFollowerState(0, -1)
	}
	rf.mu.Unlock()
	go func() {
		for {
			rf.mu.Lock()
			state := rf.serverState
			rf.mu.Unlock()
			switch {
			case state == FOLLOWER:
				rf.runAsFollower()
			case state == CANDIDATE:
				rf.runAsCandidate()
			case state == LEADER:
				rf.runAsLeader()
				time.Sleep(150 * time.Millisecond)
			}
		}
	}()

	return rf
}
