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
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	persister       *Persister          // Object to hold this peer's persisted state
	me              int                 // this peer's index into peers[]
	dead            int32               // set by Kill()
	serverState     ServerState
	currentTerm     int
	votedFor        int
	log             []LogEntry
	commitIndex     int
	lastApplied     int
	nextIndex       []int
	matchIndex      []int
	electionTimeOut bool
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
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
	rf.electionTimeOut = false

	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	reply.Term = currentTerm
	if args.Term < currentTerm {
		// requestor is more outdated than self
		reply.Success = false
	} else if args.PrevLogIndex != -1 && (len(rf.log) <= args.PrevLogIndex || (len(rf.log) > args.PrevLogIndex && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm)) {
		// there doesn't exist an entry in rf.log that has a term consistent with requestor
		reply.Success = false
	} else {
		log.Printf("[Term %d] Server %d Appending entry from %d...", args.Term, rf.me, args.LeaderID)
		// the invariant here is: rf.log and requestor's log are consistent at and before prevLogIndex
		prevLogs := rf.log[:args.PrevLogIndex+1]
		rf.mu.Lock()
		rf.log = append(prevLogs, args.Entries...)
		reply.Success = true
		if rf.serverState != FOLLOWER {
			go func() {
				rf.convertToFollower()
			}()
		}
		rf.mu.Unlock()
	}
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
	log.Printf("Server %d [Term = %d] received vote request from %d [Term = %d]", rf.me, rf.currentTerm, args.CandidateID, args.Term)
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	reply.Term = currentTerm
	if args.Term < currentTerm { // requestor is stale
		reply.VoteGranted = false
		return
	}

	if currentTerm < args.Term {
		rf.mu.Lock()
		rf.votedFor = -1
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	votedFor := rf.votedFor
	lastIdx := len(rf.log) - 1
	lastTerm := rf.log[lastIdx].Term
	rf.mu.Unlock()
	if votedFor == -1 || votedFor == args.CandidateID { // see which server's log is more up-to-date
		reply.VoteGranted = lastTerm < args.LastLogTerm || (lastTerm == args.LastLogTerm && lastIdx <= args.LastLogIndex)
	}
	log.Printf("Server %d granting vote to Server %d: %t", rf.me, args.CandidateID, reply.VoteGranted)
	if reply.VoteGranted {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
		rf.mu.Unlock()
		rf.electionTimeOut = false
	}
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

func sleepForRandomTime() {
	randNum := (rand.Intn(4)*50 + 200)
	time.Sleep(time.Duration(randNum) * time.Millisecond) // sleeps for [1000, 5000) ms
}

func (rf *Raft) convertToFollower() bool {
	rf.mu.Lock()
	rf.serverState = FOLLOWER
	rf.mu.Unlock()
	log.Printf("[Term %d] Server %d converted to follower.", rf.currentTerm, rf.me)
	for {
		rf.electionTimeOut = true
		sleepForRandomTime()
		rf.mu.Lock()
		state := rf.serverState
		rf.mu.Unlock()
		if rf.electionTimeOut && state == FOLLOWER {
			log.Printf("[Term %d] Time elapsed, Server %d converting to candidate", rf.currentTerm, rf.me)
			go func() {
				rf.convertToCandidate()
			}()
			return true
		} else if state != FOLLOWER {
			return true
		}
	}
}

func (rf *Raft) startElection() bool {
	rf.mu.Lock()
	rf.currentTerm++
	currentTerm := rf.currentTerm
	// vote for self
	rf.votedFor = rf.me
	rf.mu.Unlock()
	rf.electionTimeOut = false
	lastIdx := len(rf.log) - 1
	lastTerm := rf.log[lastIdx].Term
	voteCount := 1
	for peerID := range rf.peers {
		if peerID == rf.me {
			continue
		}
		log.Printf("[Term %d] Server %d sending request vote to %d", currentTerm, rf.me, peerID)
		args, reply := RequestVoteArgs{currentTerm, rf.me, lastIdx, lastTerm}, RequestVoteReply{}
		ok := rf.sendRequestVote(peerID, &args, &reply)
		if !ok {
			log.Printf("Failed sending RequestVote")
		}
		if reply.VoteGranted {
			voteCount++
		}
	}
	if float64(voteCount) >= float64(len(rf.peers))/2 {
		log.Printf("[Term %d] Server %d got votes from majority, converting to Leader.", currentTerm, rf.me)
		go func() {
			rf.convertToLeader()
		}()
	} else {
		log.Printf("[Term %d] Server %d didn't get votes from majority, converting to Follower.", currentTerm, rf.me)
		go func() {
			rf.convertToFollower()
		}()
	}
	return true
}

func (rf *Raft) convertToCandidate() bool {
	rf.mu.Lock()
	rf.serverState = CANDIDATE
	rf.mu.Unlock()
	log.Printf("[Term %d] Server %d converted to candidate.", rf.currentTerm, rf.me)
	// request vote from others

	for {
		rf.electionTimeOut = true
		sleepForRandomTime()
		rf.mu.Lock()
		state := rf.serverState
		rf.mu.Unlock()
		if rf.electionTimeOut && state == CANDIDATE {
			log.Printf("[Term %d] Time elapsed, starting election Server %d", rf.currentTerm, rf.me)
			rf.startElection()
		} else if state != CANDIDATE {
			return true
		}
	}
}

func (rf *Raft) convertToLeader() bool {
	rf.mu.Lock()
	rf.serverState = LEADER
	rf.mu.Unlock()
	log.Printf("[Term %d] Server %d promoted as leader.", rf.currentTerm, rf.me)
	for {
		for peerID := range rf.peers {
			rf.mu.Lock()
			currentTerm := rf.currentTerm
			state := rf.serverState
			rf.mu.Unlock()
			if state != LEADER {
				return true
			}
			if peerID == rf.me {
				continue
			}
			args, reply := &AppendEntriesArgs{Term: currentTerm, LeaderID: rf.me, PrevLogIndex: 0}, &AppendEntriesReply{}
			log.Printf("[Term %d] Server %d Sending initial heartbeat to %d ...", currentTerm, rf.me, peerID)
			rf.sendAppendEntries(peerID, args, reply)
			if !reply.Success {
				return false
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
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
	log.Printf("Make Server %d", rf.me)
	// Your initialization code here (2A, 2B, 2C).
	rf.log = make([]LogEntry, 0)
	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term: 0})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.serverState == "" {
		go func() {
			rf.convertToFollower()
		}()
	}

	state, _ := rf.GetState()
	log.Printf("Server %d has state %s.", rf.me, string(state))
	return rf
}
