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
		if gid == 0 {
			continue
		}
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
		if gid == 0 {
			continue
		}
		newGroups[gid] = servers
	}
	for _, gid := range gids {
		delete(newGroups, gid)
	}
	return newGroups
}

// return map of gid --> array of shards, shards of deleted groups are assigned to group 0
func (sm *ShardMaster) getShardAssign(currShards [NShards]int, groups map[int][]string) map[int][]int {
	shardAssign := make(map[int][]int)
	for gid := range groups {
		shardAssign[gid] = []int{}
	}
	for shard, gid := range currShards {
		if _, ok := groups[gid]; !ok { // group is deleted
			shardAssign[0] = append(shardAssign[0], shard)
		} else {
			shardAssign[gid] = append(shardAssign[gid], shard)
		}
	}
	return shardAssign
}

// given a group mapping, return an array indexed by shard numbers with balanced group assignment
func (sm *ShardMaster) rebalanceShards(groups map[int][]string) [NShards]int {
	nGroups := len(groups)
	if nGroups == 0 {
		return [NShards]int{}
	}
	newAssign := sm.getShardAssign(sm.config[len(sm.config)-1].Shards, groups)
	maxLoad := int(math.Ceil(NShards / float64(nGroups)))
	minLoad := int(math.Floor(NShards / float64(nGroups)))
	// log.Printf("pre assign = %v", newAssign)
	for gid := range newAssign {
		for len(newAssign[gid]) > maxLoad || (gid == 0 && len(newAssign[gid]) > 0) {
			// move one shard to one of the underloaded groups
			for gidNew := range newAssign {
				if gidNew != 0 && len(newAssign[gidNew]) < maxLoad {
					newAssign[gidNew] = append(newAssign[gidNew], newAssign[gid][0])
					newAssign[gid] = newAssign[gid][1:]
					break
				}
			}

		}
		if gid == 0 {
			delete(newAssign, 0)
		}
	}
	for gid := range newAssign {
		for len(newAssign[gid]) < minLoad {
			// move one shard to one of the underloaded groups
			for gidNew := range newAssign {
				if len(newAssign[gidNew]) > minLoad {
					newAssign[gid] = append(newAssign[gid], newAssign[gidNew][0])
					newAssign[gidNew] = newAssign[gidNew][1:]
					break
				}
			}

		}
		if gid == 0 {
			delete(newAssign, 0)
		}
	}
	// log.Printf("after assign = %v", newAssign)
	newShardArr := [NShards]int{}
	for gid, shards := range newAssign {
		for _, shard := range shards {
			newShardArr[shard] = gid
		}
	}
	return newShardArr
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// log.Printf("Join(%v)", args)
	// sm.mu.Lock()
	// configNum := len(sm.config)
	// newGroups := sm.addNewGroups(args.Servers)
	// newShards := sm.rebalanceShards(newGroups)
	// sm.mu.Unlock()
	// config := Config{configNum, newShards, newGroups}
	op := Op{args.ClientID, args.Serial, JOIN, map[int][]string{}, []int{}, -1, -1, -1}
	op.JoinServers = args.Servers
	ok := sm.handleRequest(op)
	if ok {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// // log.Printf("Leave(%v)", args)
	// sm.mu.Lock()
	// configNum := len(sm.config)
	// newGroups := sm.removeGroups(args.GIDs)
	// newShards := sm.rebalanceShards(newGroups)
	// sm.mu.Unlock()
	// config := Config{configNum, newShards, newGroups}
	op := Op{args.ClientID, args.Serial, LEAVE, map[int][]string{}, []int{}, -1, -1, -1}
	op.LeaveGIDs = args.GIDs
	ok := sm.handleRequest(op)
	if ok {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// sm.mu.Lock()
	// configNum := len(sm.config)
	// groups := sm.config[len(sm.config)-1].Groups
	// shards := sm.config[len(sm.config)-1].Shards
	// sm.mu.Unlock()
	// shards[args.Shard] = args.GID
	// config := Config{configNum, shards, groups}
	op := Op{args.ClientID, args.Serial, MOVE, map[int][]string{}, []int{}, -1, -1, -1}
	op.MoveShard = args.Shard
	op.MoveGID = args.GID
	ok := sm.handleRequest(op)
	if ok {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// // log.Printf("Got Query request %v", args)
	op := Op{args.ClientID, args.Serial, QUERY, map[int][]string{}, []int{}, -1, -1, -1}
	op.QueryConfigNum = args.Num
	ok := sm.handleRequest(op)
	if ok {
		sm.mu.Lock()
		index := args.Num
		if index == -1 || index >= len(sm.config) {
			index = len(sm.config) - 1
		}
		config := sm.config[index]
		sm.mu.Unlock()
		reply.Err = OK
		reply.Config = config
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (sm *ShardMaster) apply(op Op) {
	if op.Type == QUERY {
		return
	}
	configNum := len(sm.config)
	newGroups := map[int][]string{}
	newShards := [NShards]int{}
	config := Config{}
	if op.Type == JOIN {
		newGroups = sm.addNewGroups(op.JoinServers)
		newShards = sm.rebalanceShards(newGroups)

	} else if op.Type == LEAVE {
		newGroups = sm.removeGroups(op.LeaveGIDs)
		newShards = sm.rebalanceShards(newGroups)

	} else if op.Type == MOVE {
		newGroups = sm.config[len(sm.config)-1].Groups
		newShards = sm.config[len(sm.config)-1].Shards
		newShards[op.MoveShard] = op.MoveGID
	}
	config.Num, config.Shards, config.Groups = configNum, newShards, newGroups
	sm.config = append(sm.config, config)
	// log.Printf("config = %v", sm.config)

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

	go sm.applyCommitted()

	return sm
}
