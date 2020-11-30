package shardmaster

import (
	"math"
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
		// TODO: add equal() function for this
		if op.equal(appliedOp) {
			return true
		} else {
			return false
		}
	case <-time.After(1000 * time.Millisecond):
		return false
	}
}

// given newly added gid --> servers mapping, return a new mapping
// containing that and the existing mapping
func (sm *ShardMaster) addNewGroups(incomingGroups map[int][]string) map[int][]string {
	currGroups := sm.config[len(sm.config)-1].Groups
	newGroups := make(map[int][]string)
	for gid, servers := range currGroups {
		newGroups[gid] = servers
	}
	for gid, servers := range incomingGroups {
		newGroups[gid] = servers
	}
	return newGroups
}

// given gids to remove, return a new mapping
// containing the existing mapping except gids to remove
func (sm *ShardMaster) removeGroups(gids []int) map[int][]string {
	currGroups := sm.config[len(sm.config)-1].Groups
	newGroups := make(map[int][]string)
	for gid, servers := range currGroups {
		newGroups[gid] = servers
	}
	for _, gid := range gids {
		delete(newGroups, gid)
	}
	return newGroups
}

// given a group mapping, return an array indexed by shard numbers with balanced group assignment
func (sm *ShardMaster) rebalanceShards(groups map[int][]string) [NShards]int {
	currAssign := make(map[int][]int)
	for gid, _ := range groups {
		currAssign[gid] = []int{}
	}
	for shard, gid := range sm.config[len(sm.config)-1].Shards {
		currAssign[gid] = append(currAssign[gid], shard)
	}
	nGroups := len(groups)

	maxLoad := int(math.Ceil(NShards / float64(nGroups)))
	for gid, _ := range currAssign {
		for len(currAssign[gid]) > maxLoad {
			// move one shard to one of the underloaded groups
			for gidNew, _ := range currAssign {
				if len(currAssign[gidNew]) < maxLoad {
					currAssign[gidNew] = append(currAssign[gidNew], currAssign[gid][0])
					currAssign[gid] = currAssign[gid][1:]
					break
				}
			}
		}
	}
	newShardArr := [NShards]int{}
	for gid, shards := range currAssign {
		for _, shard := range shards {
			newShardArr[shard] = gid
		}
	}
	return newShardArr
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	configNum := len(sm.config)
	newGroups := sm.addNewGroups(args.Servers)
	rebalancedShards := sm.rebalanceShards(newGroups)
	config := Config{configNum, rebalancedShards, newGroups}
	op := Op{args.ClientID, args.Serial, APPEND, configNum, config}
	ok := sm.handleRequest(op)
	if ok {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	configNum := len(sm.config)
	newGroups := sm.removeGroups(args.GIDs)
	rebalancedShards := sm.rebalanceShards(newGroups)
	config := Config{configNum, rebalancedShards, newGroups}
	op := Op{args.ClientID, args.Serial, APPEND, configNum, config}
	ok := sm.handleRequest(op)
	if ok {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	configNum := len(sm.config)
	config := Config{configNum, sm.config[len(sm.config)-1].Shards, sm.config[len(sm.config)-1].Groups}
	config.Shards[args.Shard] = args.GID
	op := Op{args.ClientID, args.Serial, APPEND, configNum, config}
	ok := sm.handleRequest(op)
	if ok {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	configNum := args.Num
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
		sm.config = append(sm.config, op.Config)
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

	sm.config = make([]Config, 1)
	sm.config[0].Groups = map[int][]string{}

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
