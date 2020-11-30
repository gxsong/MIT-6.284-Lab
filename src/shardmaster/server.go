package shardmaster

import (
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	opChans    map[int]chan interface{} // index to Op map
	config     []Config                 // indexed by config num
	clientReqs map[int64]int64          // stores the lates serial number of each client

	persister *raft.Persister
	// maxraftstate int // snapshot if log grows this big
	killCh chan bool
}

func (sm *ShardMaster) handleRequest(op Op) bool {

	// 1. send op to raft
	index, _, ok := sm.rf.Start(op)
	if !ok {
		return false
	}
	// wait for the op to get committed

	sm.mu.Lock()
	opChan, ok := sm.opChans[index]
	if !ok {
		opChan = make(chan interface{}, 1)
		sm.opChans[index] = opChan
	}
	sm.mu.Unlock()
	// 2. wait for raft to commit log and execute command
	// 		- if the exact op is commited and applied, return values
	//		- if the wrong op is committed, return error wrong leader
	// 		- if never commited, after a timeout, return error wrong leader (raft is dead or stale)
	select {
	case appliedOp := <-opChan:
		if op == appliedOp {
			return true
		} else {
			return false
		}
	case <-time.After(1000 * time.Millisecond):
		return false
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// TODO: build op.Value, set configNum
	configNum := 0
	config := Config{}
	op := Op{args.ClientID, args.Serial, APPEND, configNum, config}
	ok := sm.handleRequest(op)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// TODO: build op.Value, set configNum
	configNum := 0
	config := Config{}
	op := Op{args.ClientID, args.Serial, APPEND, configNum, config}
	ok := sm.handleRequest(op)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// TODO: build op.Value, set configNum
	configNum := 0
	config := Config{}
	op := Op{args.ClientID, args.Serial, APPEND, configNum, config}
	ok := sm.handleRequest(op)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// TODO: build op.Value, set configNum
	configNum := 0
	config := Config{}
	op := Op{args.ClientID, args.Serial, GET, configNum, config}
	ok := sm.handleRequest(op)
	if ok {
		sm.mu.Lock()
		index := op.ConfigNum
		if index == -1 || index >= len(sm.config) {
			index = len(sm.config) - 1
		}
		config := sm.config[index]
		reply.Err = OK
		reply.Config = config
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (sm *ShardMaster) apply(op Op) {
	if op.Type == APPEND {
		append(sm.config, op.Config)
	}
}

// listening sm.applyCh for committed logs from raft
// apply ops from commited log (NOTE that the op doesn't have to belong to any request received by this server. it could be from some previous leader)
// let handler know that log has been applied
func (sm *ShardMaster) applyCommitted() {
	for {
		select {
		case <-sm.killCh:
			return
		default:
		}
		cmd := <-sm.applyCh

		// // apply snapshot
		// if cmd.IsSnapshot {
		// 	DPrintf("[sm][Server] Server %d got snapshot", sm.me)
		// 	sm.applySnapshot(cmd.Command.([]byte))
		// 	continue
		// }

		// update sm state to let handler know which ops are applied
		index, op := cmd.CommandIndex, cmd.Command.(Op)
		// Only execute op if it's serial number is the latest of the client's, for request deduplication
		// Consider the scenario when the leader committed the op but crashed before replying to the client
		// The client will got request timeout and resend the same request to a new leader. The same request will then be applied twice.

		// execute op if it's putappend, let the handler execute get, because it's harder to put err no key back to hander
		sm.mu.Lock()
		if serial, present := sm.clientReqs[op.ClientID]; !present || op.Serial > serial {
			sm.apply(op)
			sm.clientReqs[op.ClientID] = op.Serial
		}

		opChan, present := sm.opChans[index]

		if present {
			opChan <- op
		}

		// if not present it means no handlers waiting on that index

		// sm.checkSizeAndPersistSnapshot(index)
		sm.mu.Unlock()

	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	select {
	case <-sm.killCh: //if already set, consume it then resent to avoid block
	default:
	}
	sm.killCh <- true
}

// needed by shardsm tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.opChans = make(map[int]chan interface{})
	sm.clientReqs = make(map[int64]int64)
	sm.killCh = make(chan bool, 1)

	// snapshot := sm.persister.ReadSnapshot()
	// sm.applySnapshot(snapshot)

	go sm.applyCommitted()

	return sm
}
