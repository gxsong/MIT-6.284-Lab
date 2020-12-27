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
	"bytes"
	// "log"
	"math"
	"math/rand"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
)

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
	IsSnapshot   bool
}

type LogEntry struct {
	Term    int // maybe change to lastTerm which only stores the term of last log entry
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu                sync.Mutex          // Lock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	me                int                 // this peer's index into peers[]
	gid               int
	dead              int32 // set by Kill()
	serverState       ServerState
	currentTerm       int
	votedFor          int
	log               []LogEntry
	commitIndex       int
	lastApplied       int
	nextIndex         []int
	matchIndex        []int
	timer             *time.Timer
	totalVotes        int
	applyCh           chan ApplyMsg
	heartbeatRcvCh    chan bool
	voteGrantedCh     chan bool
	winElecCh         chan bool
	killCh            chan bool
	LeaderCh          chan bool // chan to notify server about state change
	lastSnapshotIndex int
	lastSnapshotTerm  int
}

func (rf *Raft) saveFollowerState(term int, voteFor int) {
	rf.currentTerm = term
	rf.serverState = FOLLOWER
	rf.votedFor = voteFor
	rf.totalVotes = 0
	rf.persist()
}

func (rf *Raft) saveCandidateState() {
	rf.serverState = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.totalVotes = 1
	rf.persist()
}

func (rf *Raft) saveLeaderState() {
	rf.serverState = LEADER
}

func (rf *Raft) runAsFollower(pid int) {
	// // DPrintf("{pid %d}[Term %d] Server %d running as follower", pid, rf.currentTerm, rf.me)
	rf.timer.Reset(time.Duration(genRandomTimeDuration()) * time.Millisecond)
	select {
	case <-rf.timer.C: // time elapsed, convert to candidate
		DPrintf("Server %d as Follower time elapsed", rf.me)
		rf.mu.Lock()
		rf.saveCandidateState()
		rf.mu.Unlock()
		return
	case <-rf.voteGrantedCh:
		DPrintf("Server %d as Follower granted vote", rf.me)
	case <-rf.heartbeatRcvCh:
		DPrintf("Server %d as Follower received heartbeat", rf.me)
	}
}

func (rf *Raft) runAsCandidate(pid int) {
	rf.timer.Reset(time.Duration(genRandomTimeDuration()) * time.Millisecond)
	for peerID := range rf.peers {
		go func(peerID int, pid int) {
			if peerID == rf.me {
				return
			}
			rf.mu.Lock()
			if rf.serverState != CANDIDATE {
				rf.mu.Unlock()
				return
			}
			lastTerm, lastIdx := rf.getLastLogTermAndIndex()
			args := RequestVoteArgs{rf.currentTerm, rf.me, lastIdx, lastTerm}
			reply := RequestVoteReply{}
			rf.mu.Unlock()
			ok := rf.sendRequestVote(peerID, &args, &reply)

			if !ok {
				DPrintf("Server %d Failed sending RequestVote to %d", rf.me, peerID)
				return
			}
			rf.mu.Lock()
			// check if state changed during RPC call
			if rf.currentTerm != args.Term || rf.serverState != CANDIDATE {
				rf.mu.Unlock()
				return
			}
			// // DPrintf("{pid %d}[Term %d] Server %d sent requestVote to Server %d, voteGranted = %t", pid, rf.currentTerm, rf.me, peerID, reply.VoteGranted)
			if reply.Term > rf.currentTerm {
				// DPrintf("[Term %d] Server %d is stale as candidate, stepping down", rf.currentTerm, rf.me)
				rf.saveFollowerState(reply.Term, -1)
				rf.mu.Unlock()
				return
			}
			if reply.VoteGranted {
				rf.totalVotes++
				if rf.totalVotes > len(rf.peers)/2 && rf.serverState == CANDIDATE {
					// DPrintf("[Term %d] Server %d got granted vote from %d, totalVotes = %d", rf.currentTerm, rf.me, peerID, rf.totalVotes)
					rf.saveLeaderState()
					rf.winElecCh <- true
				}
			}
			rf.mu.Unlock()
		}(peerID, pid)
	}
	select {
	case <-rf.timer.C:
		// in case of split vote, start a new term
		// TODO: double check if we actually need this
		rf.mu.Lock()
		rf.saveCandidateState()
		rf.mu.Unlock()
		DPrintf("Server %d as Candidate time elapsed", rf.me)
	case <-rf.winElecCh:
		rf.mu.Lock()
		_, lastIdx := rf.getLastLogTermAndIndex()
		rf.matchIndex = make([]int, len(rf.peers))
		rf.nextIndex = make([]int, len(rf.peers))
		for i := range rf.peers {
			rf.nextIndex[i] = lastIdx + 1
		}
		// // DPrintf("Server %d as Candidate won election, will start from index %v, log len is %d , exiting", rf.me, rf.nextIndex, len(rf.log))
		rf.mu.Unlock()
	case <-rf.heartbeatRcvCh:
		DPrintf("Server %d as Candidate received heartbeat", rf.me)
		rf.mu.Lock()
		rf.saveFollowerState(rf.currentTerm, -1)
		rf.mu.Unlock()
	}
}

func (rf *Raft) runAsLeader(pid int) {
	for peerID := range rf.peers {
		go func(peerID int, pid int) {
			if peerID == rf.me {
				return
			}

			rf.mu.Lock()
			// Check if this leader is stale and has been converted to follower
			// must check before sending heartbeat to each follower.
			if rf.serverState != LEADER {
				// // DPrintf("{pid %d} Server %d [Term %d] as a Leader already became a follower, exiting", pid, rf.me, rf.currentTerm)
				rf.mu.Unlock()
				return
			}

			nextIdx := rf.nextIndex[peerID]
			DPrintf("[Term %d] Server %d to peer %d, nextIdx = %d, lastSnapshotIndex = %d", rf.currentTerm, rf.me, peerID, nextIdx, rf.lastSnapshotIndex)
			if nextIdx <= rf.lastSnapshotIndex {
				// Send snapshot instead to reduce the time spent on log replication
				DPrintf("[Term %d] Server %d is sending snapshots to Server %d, index = %d", rf.currentTerm, rf.me, peerID, rf.lastSnapshotIndex)
				args, reply := InstallSnapshotArgs{}, InstallSnapshotReply{}
				args.Term = rf.currentTerm
				args.LeaderID = rf.me
				args.LastSnapshotIndex = rf.lastSnapshotIndex
				args.LastSnapshotTerm = rf.lastSnapshotTerm
				args.Snapshot = rf.persister.ReadSnapshot()
				rf.mu.Unlock()
				ok := rf.sendInstallSnapshot(peerID, &args, &reply)
				if !ok {
					DPrintf("Server %d failed sending InstallSnapshot to Server %d", rf.me, peerID)
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// check if state changed during RPC call. See "term confusion" section of the raft student guide
				// https://thesquareplanet.com/blog/students-guide-to-raft/
				if rf.currentTerm != args.Term {
					return
				}
				if reply.Term > rf.currentTerm {
					DPrintf("Server %d as leader is stale, in %d", rf.me, peerID)
					rf.saveFollowerState(reply.Term, -1)
				}

				rf.matchIndex[peerID] = args.LastSnapshotIndex
				rf.nextIndex[peerID] = args.LastSnapshotIndex + 1
				DPrintf("[Term %d] Server %d matchIndex = %d, nextIndex = %d of Server %d, index = %d", rf.currentTerm, rf.me, rf.matchIndex[peerID], rf.nextIndex[peerID], peerID, args.LastSnapshotIndex)

			} else {
				// Send Append Entries if no recently updated snapshots

				_, lastIdx := rf.getLastLogTermAndIndex()
				args, reply := AppendEntriesArgs{}, AppendEntriesReply{}
				args.Term = rf.currentTerm
				args.LeaderID = rf.me
				args.PrevLogIndex = nextIdx - 1
				if args.PrevLogIndex >= 0 {
					args.PrevLogTerm = rf.getLog(args.PrevLogIndex).Term
				}
				if nextIdx <= lastIdx {
					args.Entries = []LogEntry{}
					for _, log := range rf.log[rf.getActualIdx(rf.nextIndex[peerID]):] {
						args.Entries = append(args.Entries, log)
					}
				}
				args.LeaderCommit = rf.commitIndex
				// // DPrintf("{pid %d} Server %d sending heartbeat to Server %d, sending %d entries", pid, rf.me, peerID, len(args.Entries))
				DPrintf("[Term %d] Server %d is sending appendEntries to Server %d, args = %v", rf.currentTerm, rf.me, peerID, args)
				rf.mu.Unlock()
				ok := rf.sendAppendEntries(peerID, &args, &reply)
				if !ok {
					DPrintf("Server %d failed sending appendEntries to Server %d", rf.me, peerID)
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				// // DPrintf("{pid %d} Server %d got heartbeat reply from Server %d, reply.Term = %d, currentTerm = %d", pid, rf.me, peerID, reply.Term, rf.currentTerm)
				// check if state changed during RPC call. See "term confusion" section of the raft student guide
				// https://thesquareplanet.com/blog/students-guide-to-raft/
				if rf.currentTerm != args.Term {
					return
				}
				if reply.Term > rf.currentTerm {
					DPrintf("Server %d as leader is stale, in %d", rf.me, peerID)
					rf.saveFollowerState(reply.Term, -1)
				}
				if reply.Success {
					rf.matchIndex[peerID] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[peerID] = args.PrevLogIndex + len(args.Entries) + 1
				} else {
					rf.nextIndex[peerID] = reply.NextAttemptIdx
				}
				// DPrintf("[Term %d] Server %d as %s done sending entries, reply = %v, matchIndex = %v, nextIndex = %v, commit index = %d", rf.currentTerm, rf.me, rf.serverState, reply, rf.matchIndex, rf.nextIndex, rf.commitIndex)
				sendLogs := false
				for N := rf.commitIndex + 1; N <= lastIdx; N++ {
					if N < 0 {
						continue
					}
					count := 1 // counting leader itself
					if rf.getLog(N).Term == rf.currentTerm {
						for i := range rf.peers {
							if rf.matchIndex[i] >= N {
								count++
							}
						}
					}
					if count > len(rf.peers)/2 {
						rf.commitIndex = N
						sendLogs = true
						break
					}
				}
				// log.Printf("===CCC=== %d", 278)
				if sendLogs {
					// log.Printf("===DDD=== %d", 278)
					rf.sendToApplyCh()
					// log.Printf("===EEE=== %d", 278)
				}
			}
			DPrintf("Server %d returned from goroutine heartbeat to Server %d as a %s", rf.me, peerID, rf.serverState)
		}(peerID, pid)

	}
}

func (rf *Raft) IsLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.serverState == LEADER
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	DPrintf("Get State on Server %d, state = %s", rf.me, rf.serverState)
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.serverState == LEADER
}

func (rf *Raft) CompactLogAndSaveSnapshot(snapshot []byte, lastSnapshotIndex int) {
	rf.mu.Lock()
	// log.Printf("Server %d saving snapshot %v ending with index %d, curr log len %d", rf.me, snapshot, lastSnapshotIndex, rf.getLogicalIdx(len(rf.log)-1))
	if lastSnapshotIndex < rf.lastSnapshotIndex {
		return
	}
	rf.log = rf.log[rf.getActualIdx(lastSnapshotIndex):]
	rf.lastSnapshotIndex = lastSnapshotIndex
	rf.lastSnapshotTerm = rf.getLog(rf.lastSnapshotIndex).Term

	// raftState := rf.persister.ReadRaftState()
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.currentTerm)
	// e.Encode(rf.votedFor)
	// e.Encode(rf.log)
	// // e.Encode(rf.lastSnapshotIndex)
	// // e.Encode(rf.lastSnapshotTerm)
	// data := w.Bytes()
	// log.Printf("===AAA=== %d", 342)
	rf.persist()
	raftState := rf.persister.ReadRaftState()
	rf.persister.SaveStateAndSnapshot(raftState, snapshot)
	// log.Printf("===BBB=== %d", 342)
	rf.mu.Unlock()
	// // log.Printf("[raft]Server %d-%d done saving ending with index %d, log is %v", rf.me, rf.gid, lastSnapshotIndex, rf.log)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	// e.Encode(rf.lastSnapshotIndex)
	// e.Encode(rf.lastSnapshotTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	// d.Decode(&rf.lastSnapshotIndex)
	// d.Decode(&rf.lastSnapshotTerm)
}

func (rf *Raft) getLogicalIdx(idx int) int {
	return idx + rf.lastSnapshotIndex
}

func (rf *Raft) getActualIdx(idx int) int {
	return idx - rf.lastSnapshotIndex
}

func (rf *Raft) getLog(idx int) LogEntry {
	return rf.log[rf.getActualIdx(idx)]
}

func (rf *Raft) getLastLogTermAndIndex() (int, int) {
	lastIdx := rf.getLogicalIdx(len(rf.log) - 1)
	lastTerm := rf.currentTerm
	if lastIdx != -1 {
		lastTerm = rf.getLog(lastIdx).Term
	}
	return lastTerm, lastIdx
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
	Term           int
	Success        bool
	NextAttemptIdx int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %d [Term %d] received heartbeat from Server %d [Term %d], request = %v", rf.me, rf.currentTerm, args.LeaderID, args.Term, args)
	_, lastIdx := rf.getLastLogTermAndIndex()
	if args.Term < rf.currentTerm {
		DPrintf("Server %d [Term %d] received heartbeat from Stale Leader Server %d [Term %d]", rf.me, rf.currentTerm, args.LeaderID, args.Term)
		// requestor is stale, no need to reset timer
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.NextAttemptIdx = lastIdx + 1
		return
	}
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		// Note that when using saveFollowerState(), the rf may receive signal from heartbeatRcvCh before the state is changed to follower,
		// resulting in the server falsly starting new round of election as candidate while staying at the previous term
		// rf.saveFollowerState(args.Term, -1)
		rf.serverState = FOLLOWER
		rf.totalVotes = 0
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	// TODO: try unlock and then lock?
	rf.heartbeatRcvCh <- true

	// reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if lastIdx < args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.NextAttemptIdx = lastIdx + 1
		return
	}

	// Follower's lastSnapshotIndex > its nextIdx stored in leader (which is args.PrevLogIndex + 1)
	if rf.getActualIdx(args.PrevLogIndex) < 0 {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.NextAttemptIdx = rf.lastSnapshotIndex + 1
		return
	}

	reply.NextAttemptIdx = args.PrevLogIndex
	if args.PrevLogIndex > 0 && rf.getLog(args.PrevLogIndex).Term != args.PrevLogTerm {
		term := rf.getLog(args.PrevLogIndex).Term

		for reply.NextAttemptIdx > rf.getLogicalIdx(0) && rf.getLog(reply.NextAttemptIdx).Term == term {
			reply.NextAttemptIdx--
		}
		reply.NextAttemptIdx++
	} else {
		i := args.PrevLogIndex + 1
		incomingIdx := 0
		hasConflict := false
		for i < rf.getLogicalIdx(len(rf.log)) && incomingIdx < len(args.Entries) {
			if rf.getLog(i).Term != args.Entries[incomingIdx].Term {
				hasConflict = true
				break
			}
			i++
			incomingIdx++
		}
		if hasConflict || len(rf.log[rf.getActualIdx(args.PrevLogIndex+1):]) < len(args.Entries) {
			rf.log = append(rf.log[:rf.getActualIdx(args.PrevLogIndex+1)], args.Entries...)
			rf.persist()
		}
		DPrintf("Server %d [Term %d] has log entries %v", rf.me, rf.currentTerm, rf.log)
		// set commitIndex
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.getLogicalIdx(len(rf.log)-1))))
			rf.sendToApplyCh()
		}
		reply.Success = true
	}
}

func (rf *Raft) sendToApplyCh() {
	// log.Printf("===FFF=== %d", 278)
	DPrintf("Server %d sending %v to applyCh lastApplied = %d commitIndex = %d", rf.me, rf.log[rf.getActualIdx(rf.lastApplied+1):rf.getActualIdx(rf.commitIndex+1)], rf.lastApplied, rf.commitIndex)
	// DPrintf("Server %d [Term %d] has log entries %v", rf.me, rf.currentTerm, rf.log)
	if rf.gid != -1 && rf.lastApplied+1 < rf.commitIndex {
		// log.Printf("[raft]Server %d-%d sending %d-%d to applyCh", rf.me, rf.gid, rf.lastApplied+1, rf.commitIndex)
	}
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		if rf.gid != -1 && rf.lastApplied+1 < rf.commitIndex {
			// log.Printf("[raft]Server %d-%d sending %d to applyCh", rf.me, rf.gid, i)
		}
		if rf.gid != -1 {
			// log.Printf("===XXX=== %d", 540)
		}
		rf.applyCh <- ApplyMsg{true, rf.getLog(i).Command, i, false}
		if rf.gid != -1 {
			// log.Printf("===YYY=== %d", 540)
		}

	}
	if rf.gid != -1 && rf.lastApplied+1 < rf.commitIndex {
		// log.Printf("[raft]Server %d-%d done sending %d-%d to applyCh", rf.me, rf.gid, rf.lastApplied+1, rf.commitIndex)
	}
	rf.lastApplied = rf.commitIndex
	// log.Printf("===GGG=== %d", 278)
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
	rf.mu.Lock()
	DPrintf("Server %d [Term = %d] received vote request from %d [Term = %d]", rf.me, rf.currentTerm, args.CandidateID, args.Term)
	if args.Term < rf.currentTerm { // requestor is stale
		DPrintf("Requestor is stale")
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		DPrintf("Already voted for others in the same term")
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.saveFollowerState(args.Term, -1)
		DPrintf("Server %d is stale and received request vote, converting to follower", rf.me)
	}

	lastTerm, lastIdx := rf.getLastLogTermAndIndex()

	if args.LastLogTerm < lastTerm || args.LastLogTerm == lastTerm && args.LastLogIndex < lastIdx {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.voteGrantedCh <- true
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
// example InstallSnapshot RPC arguments structure.
// field names must start with capital letters!
//
type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastSnapshotIndex int
	LastSnapshotTerm  int
	Snapshot          []byte
}

//
// example InstallSnapshot RPC reply structure.
// field names must start with capital letters!
//
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	// log.Printf("[raft][install snapshot handler]Server %d-%d args.LastSnapshotIndex: %d", rf.me, rf.gid, args.LastSnapshotIndex)
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("Server %d [Term %d] received snapshot from Stale Leader Server %d [Term %d]", rf.me, rf.currentTerm, args.LeaderID, args.Term)
		// requestor is stale, no need to reset timer
		return
	}
	if args.Term > rf.currentTerm {
		// Note that when using saveFollowerState(), the rf may receive signal from heartbeatRcvCh before the state is changed to follower,
		// resulting in the server falsly starting new round of election as candidate while staying at the previous term
		// rf.saveFollowerState(args.Term, -1)
		rf.serverState = FOLLOWER
		rf.totalVotes = 0
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	rf.heartbeatRcvCh <- true

	// check snapshot may expired by lock competition, otherwise rf.logs may overflow below
	if args.LastSnapshotIndex <= rf.lastSnapshotIndex {
		return
	}
	lastTerm, lastIdx := rf.getLastLogTermAndIndex()
	if args.LastSnapshotIndex < lastIdx && args.LastSnapshotTerm == lastTerm {
		rf.log = rf.log[rf.getActualIdx(args.LastSnapshotIndex+1):]
	} else {
		// save a dummy log for follower AppendEntires consistency check
		rf.log = []LogEntry{LogEntry{args.LastSnapshotTerm, nil}}
	}

	// update snapshot state and persist them
	rf.lastSnapshotIndex = args.LastSnapshotIndex
	rf.lastSnapshotTerm = args.LastSnapshotTerm
	rf.persist()
	raftState := rf.persister.ReadRaftState()
	rf.persister.SaveStateAndSnapshot(raftState, args.Snapshot)

	// force the follower's log catch up with leader
	// log.Printf("[raft][install snapshot handler]Server %d-%d lastApplied prev: %d", rf.me, rf.gid, rf.lastApplied)
	rf.commitIndex = int(math.Max(float64(rf.commitIndex), float64(args.LastSnapshotIndex)))
	rf.lastApplied = int(math.Max(float64(rf.lastApplied), float64(args.LastSnapshotIndex)))
	// log.Printf("[raft][install snapshot handler]Server %d-%d lastApplied after: %d", rf.me, rf.gid, rf.lastApplied)

	// tell kv server to apply snapshot to its storage
	DPrintf("[Term %d] Server %d sending snapshot to applych", rf.currentTerm, rf.me)
	rf.applyCh <- ApplyMsg{true, args.Snapshot, -1, true}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[Term %d] Server %d received Command %v as %s", rf.currentTerm, rf.me, command, rf.serverState)
	term, index, isLeader := -1, -1, rf.serverState == LEADER
	if isLeader {
		rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
		term = rf.currentTerm
		index = rf.getLogicalIdx(len(rf.log) - 1)
		rf.persist()
	}

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
	DPrintf("Kill() at Raft %d", rf.me)
	select {
	case <-rf.killCh: //if already set, consume it then resent to avoid block
	default:
	}
	DPrintf("before killCh at Raft %d", rf.me)
	rf.killCh <- true
	DPrintf("Killing raft %d", rf.me)
	// original implementation below
	// atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	// original implementation below. NOt used
	// z := atomic.LoadInt32(&rf.dead)
	// return z == 1
	return false
}

func genRandomTimeDuration() int {
	return rand.Intn(200) + 300
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
func Make(peers []*labrpc.ClientEnd, me int, gid int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.gid = gid
	rf.applyCh = applyCh
	DPrintf("Make Server %d", rf.me)
	rf.heartbeatRcvCh = make(chan bool, 100)
	rf.voteGrantedCh = make(chan bool, 100)
	rf.winElecCh = make(chan bool, 100)
	rf.killCh = make(chan bool, 1)
	rf.LeaderCh = make(chan bool, 1)
	rf.mu.Lock()
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.totalVotes = 0
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{-1, 0})
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.serverState = FOLLOWER
	// read perviously persisted state
	rf.readPersist(rf.persister.ReadRaftState())
	rf.persist()
	rf.mu.Unlock()
	rf.timer = time.NewTimer(time.Duration(genRandomTimeDuration()) * time.Millisecond)
	pid := rand.Int() % 1000
	go func(pint int) {
		var prevState ServerState
		var state ServerState
		for {
			select {
			case <-rf.killCh:
				DPrintf("Raft %d killed", rf.me)
				return
			default:
				// DPrintf("Raft %d still alive", rf.me)
			}

			rf.mu.Lock()
			state = rf.serverState
			rf.mu.Unlock()
			if gid != -1 { // only do this for shardKV
				if prevState != LEADER && state == LEADER {
					// // log.Printf("[raft]Server %d-%d sending to leaderCh %t", rf.me, rf.gid, true)
					rf.LeaderCh <- true
				} else if prevState == LEADER && state != LEADER {
					// // log.Printf("[raft]Server %d-%d sending to leaderCh %t", rf.me, rf.gid, false)
					rf.LeaderCh <- false
				}
			}
			prevState = state
			switch {
			case state == FOLLOWER:
				rf.runAsFollower(pid)
			case state == CANDIDATE:
				rf.runAsCandidate(pid)
			case state == LEADER:
				rf.runAsLeader(pid)
				time.Sleep(120 * time.Millisecond)
			}
		}

	}(pid)
	return rf
}
