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

type ToFollower struct {
	convert  chan bool
	term     chan int
	votedFor chan int
}

type ToCandidate struct {
	convert chan bool
}

type ToLeader struct {
	convert chan bool
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
	toFollower  ToFollower
	toCandidate ToCandidate
	toLeader    ToLeader
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
	log.Printf("Server %d running as follower", rf.me)
	rf.timer.Reset(time.Duration(genRandomTimeDuration()) * time.Millisecond)
	select {
	case <-rf.timer.C: // time elapsed, convert to candidate
		log.Printf("Server %d as Follower time elapsed", rf.me)
		rf.saveCandidateState()
		return
	case <-rf.toFollower.convert: // either got heartbeat from AppendEntries handler, or voted for others in RequestVote handler
		rf.saveFollowerState(<-rf.toFollower.term, <-rf.toFollower.votedFor)
		return
	}
}

func (rf *Raft) runAsCandidate() {
	log.Printf("Server %d running as candidate", rf.me)
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
		log.Printf("[Term %d] Server %d sending requestVote", rf.currentTerm, rf.me)
		rf.mu.Unlock()
		for peerID := range rf.peers {
			go func(peerID int) {
				if peerID == rf.me {
					return
				}
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(peerID, &args, &reply)

				if !ok {
					log.Printf("Failed sending RequestVote")
					return
				}
				rf.mu.Lock()
				log.Printf("[Term %d] Server %d sent requestVote to Server %d, voteGranted = %t", rf.currentTerm, rf.me, peerID, reply.VoteGranted)
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.toFollower.convert <- true
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
						rf.toLeader.convert <- true
					}
				}
				rf.mu.Unlock()
			}(peerID)
		}
	}()
	select {
	case <-rf.timer.C:
		log.Printf("Server %d as Candidate time elapsed", rf.me)
		rf.saveCandidateState()
		return
	case <-rf.toFollower.convert:
		log.Printf("Server %d as Candidate converting to Follower", rf.me)
		rf.mu.Lock()
		rf.saveFollowerState(rf.currentTerm, -1)
		rf.mu.Unlock()
		return
	case <-rf.toLeader.convert:
		log.Printf("Server %d as Candidate converting to Leader", rf.me)
		rf.saveLeaderState()
		return
	}
}

func (rf *Raft) runAsLeader() {
	log.Printf("Server %d running as leader", rf.me)
	for peerID := range rf.peers {
		go func(peerID int) {
			if peerID == rf.me {
				return
			}
			// TODO: populate arg fields for actual log update, currently only for heartbeat
			rf.mu.Lock()
			args, reply := AppendEntriesArgs{Term: rf.currentTerm, LeaderID: rf.me, PrevLogIndex: 0}, AppendEntriesReply{}
			rf.mu.Unlock()
			rf.sendAppendEntries(peerID, &args, &reply)
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.toFollower.convert <- true
				rf.mu.Unlock()
				return
			}
			// same as the race condition in runAsCandidate()
			if rf.currentTerm != args.Term || rf.serverState != LEADER {
				rf.mu.Unlock()
				return
			}
		}(peerID)
	}
	select {
	case <-rf.toFollower.convert:
		log.Printf("Server %d as Leader converting to Follower", rf.me)
		rf.mu.Lock()
		rf.saveFollowerState(rf.currentTerm, -1)
		rf.mu.Unlock()
	}
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
	log.Printf("[Verbose] Server %d reseted timer by AppendEntries", rf.me)

	// rf.mu.Lock()
	// currentTerm := rf.currentTerm
	// rf.mu.Unlock()

	// if args.Term < currentTerm {
	// 	// requestor is more outdated than self, no need to reset timer
	// 	reply.Term = currentTerm
	// 	reply.Success = false
	// 	return
	// }

	// rf.timer.Reset(time.Duration(genRandomTimeDuration()) * time.Millisecond)
	// if args.Term >= currentTerm {
	// 	rf.mu.Lock()
	// 	rf.votedFor = args.LeaderID
	// 	rf.currentTerm = args.Term
	// 	reply.Term = rf.currentTerm
	// 	rf.mu.Unlock()
	// 	go func() {
	// 		rf.convertToFollower()
	// 	}()
	// }

	// if args.PrevLogIndex != -1 && (len(rf.log) <= args.PrevLogIndex || (len(rf.log) > args.PrevLogIndex && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm)) {
	// 	// there doesn't exist an entry in rf.log that has a term consistent with requestor
	// 	reply.Success = false
	// 	return
	// }

	// log.Printf("[Term %d] Server %d Appending entry from %d...", args.Term, rf.me, args.LeaderID)
	// // the invariant here is: rf.log and requestor's log are consistent at and before prevLogIndex
	// rf.mu.Lock()
	// // TODO: fix log replication logic
	// prevLogs := rf.log[:args.PrevLogIndex+1]
	// rf.log = append(prevLogs, args.Entries...)
	// reply.Success = true
	// rf.mu.Unlock()
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
	log.Printf("Server %d [Term = %d] received vote request from %d [Term = %d]", rf.me, rf.currentTerm, args.CandidateID, args.Term)
	if args.Term < rf.currentTerm { // requestor is stale
		log.Println("Requestor is stale")
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.saveFollowerState(args.Term, -1)
		log.Printf("Server %d, args.Term > rf.currentTerm", rf.me)
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

	log.Printf("[Term %d] Server %d voted for Server %d: %t", rf.currentTerm, rf.me, args.CandidateID, reply.VoteGranted)
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

func (rf *Raft) convertToFollower() bool {
	rf.mu.Lock()
	rf.serverState = FOLLOWER
	log.Printf("[Term %d] Server %d converted to follower.", rf.currentTerm, rf.me)
	rf.mu.Unlock()
	rf.timer.Reset(time.Duration(genRandomTimeDuration()) * time.Millisecond)
	// TODO: need reset timer?
	for range rf.timer.C {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		log.Printf("%d [Term %d] Time elapsed, Server %d converting to candidate", time.Now().UnixNano()/1e6, rf.currentTerm, rf.me)
		if rf.serverState == FOLLOWER {
			go func() {
				rf.convertToCandidate()
			}()
			return true
		}

	}
	return true
}

func (rf *Raft) startElection() bool {
	rf.mu.Lock()
	rf.currentTerm++
	currentTerm := rf.currentTerm
	// vote for self
	rf.votedFor = rf.me
	rf.mu.Unlock()

	// log.Printff("[Verbose] Server %d reseted timer by StartElection", rf.me)
	rf.timer.Reset(time.Duration(genRandomTimeDuration()) * time.Millisecond)
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
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			go func() {
				rf.convertToFollower()
			}()
			rf.mu.Unlock()
			log.Printf("1111")
			return true
		}
		if rf.currentTerm != args.Term || rf.serverState != CANDIDATE {
			rf.mu.Unlock()
			log.Printf("2222")
			return true
		}
		rf.mu.Unlock()
		log.Printf("3333")
		log.Printf("[Verbose] peerID = %d, Server %d, reply.Term = %d, currentTerm = %d, reply.VoteGranted = %t", peerID, rf.me, reply.Term, currentTerm, reply.VoteGranted)
		if reply.Term == currentTerm && reply.VoteGranted {
			voteCount++
		}
	}
	rf.mu.Lock()
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
	rf.mu.Unlock()
	return true
}

func (rf *Raft) convertToCandidate() bool {
	rf.timer.Reset(time.Duration(genRandomTimeDuration()) * time.Millisecond)
	rf.mu.Lock()
	rf.serverState = CANDIDATE
	log.Printf("[Term %d] Server %d converted to candidate.", rf.currentTerm, rf.me)
	rf.mu.Unlock()
	// request vote from others

	for range rf.timer.C {
		rf.mu.Lock()
		state := rf.serverState
		log.Printf("[Term %d] Time elapsed, starting election Server %d", rf.currentTerm, rf.me)
		rf.mu.Unlock()
		if state == CANDIDATE {
			rf.startElection()
		}
		return true
	}
	return true
}

func (rf *Raft) convertToLeader() bool {
	rf.mu.Lock()
	rf.serverState = LEADER
	rf.mu.Unlock()
	// log.printf("%d [Term %d] Server %d promoted as leader.", time.Now().UnixNano()/1e6, rf.currentTerm, rf.me)
	for {
		for peerID := range rf.peers {
			rf.mu.Lock()
			currentTerm := rf.currentTerm
			rf.mu.Unlock()
			if peerID == rf.me {
				continue
			}
			args, reply := &AppendEntriesArgs{Term: currentTerm, LeaderID: rf.me, PrevLogIndex: 0}, &AppendEntriesReply{}
			// log.printf("%d [Term %d] Server %d Sending initial heartbeat to %d ...", time.Now().UnixNano()/1e6, currentTerm, rf.me, peerID)
			rf.sendAppendEntries(peerID, args, reply)
			if reply.Term > currentTerm {
				rf.mu.Lock()
				rf.currentTerm = reply.Term
				go func() {
					rf.convertToFollower()
				}()
				rf.mu.Unlock()
				return true
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
}

// func (rf *Raft) resetTimer() {
// 	log.Printf("Server %d reset timer", rf.me)
// 		randNum := (rand.Intn(300) + 200)
// 		// log.Printf("[Verbose] %d", randNum)
// 		time.Sleep(time.Duration(randNum) * time.Millisecond) // sleeps for [1000, 5000) ms
// 		switch {
// 		case <-rf.timer.reset:
// 			continue
// 		default:
// 			log.Printf("Server %d time elapsed", rf.me)
// 			rf.timer.elapse <- true
// 		}
// 	}
// }

func genRandomTimeDuration() int {
	return rand.Intn(100) + 50
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
	rf.totalVotes = 0
	rf.toFollower.convert = make(chan bool)
	rf.toCandidate.convert = make(chan bool)
	rf.toLeader.convert = make(chan bool)
	// rf.log = append(rf.log, LogEntry{Term: 0})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.timer = time.NewTimer(time.Duration(genRandomTimeDuration()) * time.Millisecond)

	if rf.serverState == "" {
		rf.saveFollowerState(0, -1)
	}

	go func() {
		for {
			log.Println("==============")
			rf.mu.Lock()
			state := rf.serverState
			rf.mu.Unlock()
			switch {
			case state == FOLLOWER:
				log.Printf("Server %d Converting to Follower", rf.me)
				rf.runAsFollower()
				log.Printf("Server %d returned from follower,now has state %s", rf.me, rf.serverState)
			case state == CANDIDATE:
				log.Printf("Server %d Converting to Candidate", rf.me)
				rf.runAsCandidate()
			case state == LEADER:
				log.Printf("Server %d Converting to Leader", rf.me)
				rf.runAsLeader()
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	return rf
}
