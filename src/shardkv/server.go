package shardkv

// import "../shardmaster"
import (
	"bytes"

	// "log"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
)

type ShardKV struct {
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	sm       *shardmaster.Clerk
	applyCh  chan raft.ApplyMsg
	make_end func(string) *labrpc.ClientEnd
	gid      int
	masters  []*labrpc.ClientEnd // shardmaster servers

	currConfigNum int
	shardsOwned   []ShardState             // shards under newest config, might not be in shardedDB yet
	opChans       map[int]chan interface{} // index to Op map
	shardedDB     map[int]DB               // shardNum --> DB store
	clientReqs    map[int]ClientReqLog     // stores the lates serial number of each client by shards
	groupReqs     map[int]GroupReqLog      // otherGID --> {configNum --> {shardNum --> recved}}} stores the move shard requests received from other groups

	persister    *raft.Persister
	maxraftstate int // snapshot if log grows this big
	killCh       chan bool
	isLeader     bool
	syncConfLock sync.Mutex
}

func (kv *ShardKV) commitOpToAllServers(op Op) Err {
	// log.Printf("Server %d-%d Got %s op", kv.me, kv.gid, op.Type)
	// 1. send op to raft

	index, _, ok := kv.rf.Start(op.copy())
	// kv.mu.Lock()
	// kv.isLeader = ok
	// kv.mu.Unlock()
	if !ok {
		// log.Printf("Server %d-%d can't apply op, not leader", kv.me, kv.gid)
		return ErrWrongLeader
	}
	// wait for the op to get committed
	// TODO: locking issue?
	// DPrintf("Server %d Handle request before locking", kv.me)
	kv.mu.Lock()
	// // log.Printf("Server %d-%d acquired lock in commitOpToAllServers", kv.me, kv.gid)
	opChan, ok := kv.opChans[index]
	if !ok {
		opChan = make(chan interface{}, 1)
		kv.opChans[index] = opChan
	}
	kv.mu.Unlock()
	// log.Printf("Server %d-%d got opchan at index %d", kv.me, kv.gid, index)
	// // log.Printf("Server %d-%d released lock in commitOpToAllServers", kv.me, kv.gid)
	select {
	case appliedOp := <-opChan:
		// DPrintf("[kv][Server] Server %d applied op %v, op sent:%v", kv.me, appliedOp, op)
		if op.equal(appliedOp) {
			// log.Printf("Server %d-%d applied %s op", kv.me, kv.gid, op.Type)
			if (op.Type == GET || op.Type == PUT || op.Type == APPEND) && !kv.ownsShard(op.Shard) {
				return ErrWrongGroup
			}
			return OK
		} else {
			// log.Printf("Server %d-%d didn't apply %s op", kv.me, kv.gid, op.Type)
			return ErrWrongLeader
		}
	case <-time.After(1000 * time.Millisecond):
		// DPrintf("[kv][Server] Server %d Request timed out %v", kv.me, op)
		// log.Printf("Server %d-%d timedout applying %s op", kv.me, kv.gid, op.Type)
		return ErrWrongLeader
	}

}

func (kv *ShardKV) ownsShard(shard int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// log.Printf("Server %d-%d shard state %v", kv.me, kv.gid, kv.shardsOwned)
	return kv.shardsOwned[shard] == VALID
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// log.Printf("[kv][Server] Server %d-%d got Get Request %v", kv.me, kv.gid, args)
	// kv.syncConfLock.Lock()
	// // log.Printf("Server %d-%d acquired syncConfigLock in Get", kv.me, kv.gid)
	// upon receiving request, first check if config has changed by calling kv.sm.query()
	// if this server (replica group) is no longer in charge of the requested shard, return ErrWrongGroup
	// and call the leaders of other replica groups to migrate content
	// kv.mu.Lock()
	// if kv.parsingNewConfig {
	// 	// log.Printf("Server %d-%d can't serve requst now, updating config", kv.me, kv.gid)
	// 	reply.Err = ErrUpdatingConfig
	// 	kv.mu.Unlock()
	// 	return
	// }
	// kv.mu.Unlock()
	// if !kv.rf.IsLeader() {
	// 	reply.Err = ErrWrongLeader
	// 	return
	// }
	// if !kv.ownsShard(args.Shard) {
	// 	reply.Err = ErrWrongGroup
	// 	return
	// }
	op := Op{}
	op.ClientID, op.Serial, op.Type, op.Shard, op.Key = args.ClientID, args.Serial, args.OpType, args.Shard, args.Key
	// log.Printf("Server %d-%d committing %s op to all servers", kv.me, kv.gid, op.Type)
	err := kv.commitOpToAllServers(op)
	// log.Printf("Server %d-%d done committing %s op to all servers", kv.me, kv.gid, op.Type)
	if err == OK {
		// if !kv.ownsShard(args.Shard) {yyyyyyy
		// 	reply.Err = ErrWrongGroup
		// 	return
		// }
		kv.mu.Lock()
		// // log.Printf("Server %d-%d acquired lock in Get", kv.me, kv.gid)
		value, present := kv.shardedDB[op.Shard][op.Key]
		if !present {
			reply.Err = ErrNoKey
		} else {
			reply.Value = value
			reply.Err = OK
		}
		kv.mu.Unlock()
		// // log.Printf("Server %d-%d released lock in Get", kv.me, kv.gid)
	} else {
		reply.Err = err
	}
	// kv.syncConfLock.Unlock()
	// // log.Printf("Server %d-%d released syncConfigLock in Get", kv.me, kv.gid)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// log.Printf("[kv][Server] Server %d-%d got PutAppend Request %v", kv.me, kv.gid, args)
	// kv.syncConfLock.Lock()
	// // log.Printf("Server %d-%d acquired syncConfigLock in PutAppend", kv.me, kv.gid)
	// upon receiving request, first check if config has changed by calling kv.sm.query()
	// if this server (replica group) is no longer in charge of the requested shard, return ErrWrongGroup
	// and call the leaders of other replica groups to migrate content
	// kv.mu.Lock()
	// if kv.parsingNewConfig {
	// 	// log.Printf("Server %d-%d can't serve requst now, updating config", kv.me, kv.gid)
	// 	reply.Err = ErrUpdatingConfig
	// 	kv.mu.Unlock()
	// 	return
	// }
	// kv.mu.Unlock()

	// if !kv.rf.IsLeader() {
	// 	reply.Err = ErrWrongLeader
	// 	return
	// }
	// if !kv.ownsShard(args.Shard) {
	// 	reply.Err = ErrWrongGroup
	// 	return
	// }
	op := Op{}
	op.ClientID, op.Serial, op.Type, op.Shard, op.Key, op.Value = args.ClientID, args.Serial, args.OpType, args.Shard, args.Key, args.Value
	// log.Printf("Server %d-%d committing %s op to all servers", kv.me, kv.gid, op.Type)
	err := kv.commitOpToAllServers(op)
	// log.Printf("Server %d-%d done committing %s op to all servers", kv.me, kv.gid, op.Type)
	// if !kv.ownsShard(args.Shard) {
	// 	reply.Err = ErrWrongGroup
	// 	return
	// }
	// if ok {
	// 	reply.Err = OK
	// } else {
	// 	reply.Err = ErrWrongLeader
	// }
	reply.Err = err
	// kv.syncConfLock.Unlock()
	// // log.Printf("Server %d-%d released syncConfigLock in PutAppend", kv.me, kv.gid)
}

func (kv *ShardKV) apply(op Op) {
	// log.Printf("Server %d-%d apply(), op=%v, DB=%v", kv.me, kv.gid, op, kv.shardedDB)
	if op.Type == PUT {
		if kv.shardsOwned[op.Shard] != VALID {
			return
		}
		if serial, present := kv.clientReqs[op.Shard][op.ClientID]; !present || op.Serial > serial {
			kv.shardedDB[op.Shard][op.Key] = op.Value
			kv.clientReqs[op.Shard][op.ClientID] = op.Serial
		}
	} else if op.Type == APPEND {
		if kv.shardsOwned[op.Shard] != VALID {
			return
		}
		if serial, present := kv.clientReqs[op.Shard][op.ClientID]; !present || op.Serial > serial {
			val, present := kv.shardedDB[op.Shard][op.Key]
			if !present {
				val = ""
			}
			kv.shardedDB[op.Shard][op.Key] = val + op.Value
			kv.clientReqs[op.Shard][op.ClientID] = op.Serial
		}
	} else if op.Type == NewConfig {

		// log.Printf("Server %d-%d currconfignum %d performing NewConfig op %v", kv.me, kv.gid, kv.currConfigNum, op)
		if kv.currConfigNum == 0 {
			// initialize shards on the first config
			kv.currConfigNum = op.ConfigNum
			for shard, gid := range op.Config.Shards {
				// log.Printf("DEBUG shard %d, gid %d", shard, gid)
				if _, ok := kv.shardedDB[shard]; gid == kv.gid && !ok {
					// log.Printf("	DEBUG shard %d", shard)
					kv.shardedDB[shard] = DB{}
					kv.clientReqs[shard] = ClientReqLog{}
					kv.shardsOwned[shard] = VALID
				}
			}
		}
		kv.currConfigNum = op.ConfigNum

	} else if op.Type == SendShard {
		shardNums := op.MovingShardNums

		// log.Printf("Server %d-%d performing SendShard op %v", kv.me, kv.gid, op)
		newestConfig := op.Config
		args, reply := MoveShardArgs{kv.gid, op.DestGID, newestConfig.Num, shardNums, kv.copyDBShards(shardNums), kv.copyClientReqShards(shardNums)}, MoveShardReply{}
		kv.sendMoveShardToGroupLeader(newestConfig.Groups[op.DestGID], &args, &reply)
		kv.deleteDBShards(shardNums)
		kv.deleteClientReqShards(shardNums)
		for _, shard := range shardNums {
			kv.shardsOwned[shard] = INVALID
		}
	} else if op.Type == ReceiveShard {
		// log.Printf("Server %d-%d performing ReceiveShard op %v", kv.me, kv.gid, op)
		for shard, DB := range op.MovingShardDBs {
			if kv.shardsOwned[shard] == VALID {
				continue
			}
			kv.shardedDB[shard] = DB
		}
		for shard, clientReqs := range op.MovingShardClientReqs {
			if kv.shardsOwned[shard] == VALID {
				continue
			}
			kv.clientReqs[shard] = clientReqs
		}
		for _, shard := range op.MovingShardNums {
			kv.shardsOwned[shard] = VALID
		}
	}
}

func (kv *ShardKV) applyCommitted() {
	for {
		// // log.Printf("Server %d-%d applyCh length %d", kv.me, kv.gid, len(kv.applyCh))
		select {
		case <-kv.killCh:
			// DPrintf("Server %d killed", kv.me)
			return
		default:
			// DPrintf("Server %d still alive", kv.me)
		}
		var cmd raft.ApplyMsg
		select {
		case cmd = <-kv.applyCh:
			break
		default:
			time.Sleep(time.Millisecond * 100)
			continue
		}
		// log.Printf("===VVV=== %d", 540)
		// cmd := <-kv.applyCh
		// log.Printf("===ZZZ=== %d", 540)
		// log.Printf("Server %d-%d got command at index %d from applyCh", kv.me, kv.gid, cmd.CommandIndex)

		// apply snapshot
		if cmd.IsSnapshot {
			// DPrintf("[kv][Server] Server %d got snapshot", kv.me)
			kv.applySnapshot(cmd.Command.([]byte))
			continue
		}

		// update kv state to let handler know which ops are applied
		index, op := cmd.CommandIndex, cmd.Command.(Op)
		// Making a copy of op to avoid racing
		opCopy := op.copy()

		kv.mu.Lock()
		// // log.Printf("Server %d-%d acquired lock in applyCommitted", kv.me, kv.gid)
		kv.apply(opCopy)
		// log.Printf("Server %d-%d returned from apply() index %d %v, shardStates = %v, db = %v", kv.me, kv.gid, index, op, kv.shardsOwned, kv.shardedDB)

		opChan, present := kv.opChans[index]
		if !present {
			opChan = make(chan interface{}, 1)
			kv.opChans[index] = opChan
		}

		// if present {
		// log.Printf("[kv][Server] Server %d sending op to opChan at index %d", kv.me, index)
		opChan <- op
		// log.Printf("[kv][Server] Server %d end sending op to opChan at index %d", kv.me, index)
		// }

		// if not present it means no handlers waiting on that index

		kv.mu.Unlock()
		kv.checkSizeAndPersistSnapshot(index)
		// log.Printf("Server %d-%d released lock in applyCommitted", kv.me, kv.gid)
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	select {
	case <-kv.killCh: //if already set, consume it then resent to avoid block
	default:
	}
	kv.killCh <- true
}

func (kv *ShardKV) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.clientReqs)
	e.Encode(kv.shardedDB)
	e.Encode(kv.shardsOwned)
	return w.Bytes()
}

func (kv *ShardKV) decodeSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.clientReqs)
	d.Decode(&kv.shardedDB)
	d.Decode(&kv.shardsOwned)
}

// Check if raft state size is larger than maxraftstate
// if so, send a snapshot to the raft

// kv staste to save in snapshot:
// kv.store, kv.clientReqs
func (kv *ShardKV) checkSizeAndPersistSnapshot(index int) {
	if kv.maxraftstate == -1 {
		return
	}
	takeSnapshot := false
	snapshot := []byte{}
	if kv.persister.RaftStateSize()/10 >= kv.maxraftstate/10 { // take snapshot when appraoching limit
		// log.Printf("Server %d-%d state size is %d", kv.me, kv.gid, kv.persister.RaftStateSize())
		kv.mu.Lock()
		snapshot = kv.encodeSnapshot()
		kv.mu.Unlock()
		takeSnapshot = true
		// DPrintf("[kv][Server] server %d applying snapshot %v at index %d", kv.me, snapshot, index)
		// must spawn another goroutine, otherwise server will be blocked waiting on raft's lock

	}
	if takeSnapshot {
		go kv.rf.CompactLogAndSaveSnapshot(snapshot, index)
	}
}

// apply snapshot to kv's state
func (kv *ShardKV) applySnapshot(snapshot []byte) {
	// snapshot := kv.persister.ReadSnapshot()
	// DPrintf("[kv][Server] Serve %d applying Snapshot %v", kv.me, snapshot)
	kv.mu.Lock()
	// // log.Printf("Server %d-%d acquired lock in applySnapshot", kv.me, kv.gid)
	kv.decodeSnapshot(snapshot)
	kv.mu.Unlock()
	// // log.Printf("Server %d-%d released lock in applySnapshot", kv.me, kv.gid)
	// DPrintf("[kv][Server] Serve %d done applying Snapshot", kv.me)
}

func (kv *ShardKV) markGroupReq(fromGID int, configNum int) {
	if _, ok := kv.groupReqs[fromGID]; !ok {
		kv.groupReqs[fromGID] = make(GroupReqLog)
	}
	kv.groupReqs[fromGID][configNum] = true
}

func copyDBShard(original DB) DB {
	copy := DB{}
	for key, val := range original {
		copy[key] = val
	}

	return copy
}

func copyClientReqShard(original ClientReqLog) ClientReqLog {
	copy := ClientReqLog{}
	for client, seq := range original {
		copy[client] = seq
	}
	return copy
}

func (kv *ShardKV) copyClientReqShards(shards []int) map[int]ClientReqLog {
	copy := make(map[int]ClientReqLog)
	for _, shard := range shards {
		copy[shard] = copyClientReqShard(kv.clientReqs[shard])
	}
	return copy
}

func (kv *ShardKV) copyDBShards(shards []int) map[int]DB {
	copy := make(map[int]DB)
	for _, shard := range shards {
		copy[shard] = copyDBShard(kv.shardedDB[shard])
	}
	return copy
}

func (kv *ShardKV) deleteClientReqShards(shards []int) {
	for _, shard := range shards {
		delete(kv.shardedDB, shard)
	}
}

func (kv *ShardKV) deleteDBShards(shards []int) {
	for _, shard := range shards {
		delete(kv.clientReqs, shard)
	}
}

func (kv *ShardKV) copyShardedDB() map[int]DB {
	copy := make(map[int]DB)
	for shard, DB := range kv.shardedDB {
		copy[shard] = copyDBShard(DB)
	}
	return copy
}

func copyConfig(config shardmaster.Config) shardmaster.Config {
	copy := shardmaster.Config{}
	copy.Num = config.Num
	copy.Shards = [shardmaster.NShards]int{}
	copy.Groups = make(map[int][]string)
	for shard, gid := range config.Shards {
		copy.Shards[shard] = gid
	}
	for gid, servers := range config.Groups {
		copy.Groups[gid] = append(copy.Groups[gid], servers...)
	}
	return copy
}

func (kv *ShardKV) syncConfig() {
	for {

		select {
		case <-kv.killCh:
			// DPrintf("Server %d killed", kv.me)
			return
		default:
			// DPrintf("Server %d still alive", kv.me)
		}
		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}

		newestConfig := kv.sm.Query(-1)
		kv.mu.Lock()
		if newestConfig.Num == kv.currConfigNum {
			kv.mu.Unlock()
			continue
		}
		// log.Printf("Server %d-%d currconfigNum %d got new config %v", kv.me, kv.gid, kv.currConfigNum, newestConfig)
		// kv.parsingNewConfig = true

		kv.mu.Unlock()

		newConfigOp := Op{}
		newConfigOp.Type, newConfigOp.ConfigNum, newConfigOp.Config = NewConfig, newestConfig.Num, copyConfig(newestConfig)
		// log.Printf("Server %d-%d committing %s op to all servers", kv.me, kv.gid, newConfigOp.Type)
		err := kv.commitOpToAllServers(newConfigOp)
		// log.Printf("Server %d-%d done committing %s op to all servers", kv.me, kv.gid, newConfigOp.Type)

		if err != OK {
			continue
		}

		// kv.currConfigNum = newestConfig.Num
		// // log.Printf("Server %d-%d released lock in syncConfig", kv.me, kv.gid)

		kv.mu.Lock()

		// ======== DEBUG =======
		shardAssign := newestConfig.Shards
		shardsToAdd := []int{}
		for shardNum, gid := range shardAssign {
			if gid == kv.gid {
				// adding new shard to this group
				// TODO: apply shard data migrated from previous group
				if _, ok := kv.shardedDB[shardNum]; !ok {
					// kv.shardedDB[shardNum] = DB{}
					// kv.clientReqs[shardNum] = make(ClientReqLog)
					shardsToAdd = append(shardsToAdd, shardNum)
					kv.shardsOwned[shardNum] = PENDING
				}

			}
		}
		// ======== DEBUG =======

		shardsToRemove := map[int][]int{}
		for shardNum := range kv.shardedDB {
			// shard no longer belong to this group
			if destGid := shardAssign[shardNum]; destGid != kv.gid {
				_, ok := shardsToRemove[destGid]
				if !ok {
					shardsToRemove[destGid] = []int{}
				}
				shardsToRemove[destGid] = append(shardsToRemove[destGid], shardNum)
				kv.shardsOwned[shardNum] = INVALID
			}
		}

		kv.mu.Unlock()

		for destGid, shards := range shardsToRemove {
			sendOp := Op{}
			sendOp.Type, sendOp.DestGID, sendOp.ConfigNum, sendOp.Config, sendOp.MovingShardNums = SendShard, destGid, newestConfig.Num, copyConfig(newestConfig), shards
			// log.Printf("Server %d-%d committing %s op to all servers", kv.me, kv.gid, sendOp.Type)
			kv.commitOpToAllServers(sendOp)
			// log.Printf("Server %d-%d done committing %s op to all servers", kv.me, kv.gid, sendOp.Type)
			// log.Printf("Server %d-%d ConfigNum %d shardsToAdd %v shardsToRemove %v", kv.me, kv.gid, newestConfig.Num, shardsToAdd, shardsToRemove)
		}

		// kv.syncConfLock.Unlock()
		// // log.Printf("Server %d-%d released syncConfigLock in syncConfig", kv.me, kv.gid)
		// // log.Printf("Server %d-%d released lock in syncConfig", kv.me, kv.gid)

		// kv.mu.Lock()
		// // log.Printf("Server %d-%d end syncCOnfig(), DB=%v", kv.me, kv.gid, kv.shardedDB)
		// kv.parsingNewConfig = false
		// kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) sendMoveShardToGroupLeader(servers []string, args *MoveShardArgs, reply *MoveShardReply) bool {

	for _, server := range servers {
		srv := kv.make_end(server)
		ok := srv.Call("ShardKV.MoveShard", args, reply)
		if !ok || reply.Err != OK {
			continue
		}
		return ok
	}
	return false
}

func (kv *ShardKV) MoveShard(args *MoveShardArgs, reply *MoveShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if kv.currConfigNum > args.ConfigNum {
		kv.mu.Unlock()
		return
	}

	if args.ToGID != kv.gid {
		reply.Err = ErrWrongGroup
		// log.Printf("Server %d-%d got MoveShard request  but wront group %v", kv.me, kv.gid, args)
		kv.mu.Unlock()
		return
	}

	if _, ok := kv.groupReqs[args.FromGID][args.ConfigNum]; ok {
		// log.Printf("Server %d-%d got MoveShard request but dup request %v", kv.me, kv.gid, args)
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	kv.markGroupReq(args.FromGID, args.ConfigNum)

	kv.mu.Unlock()
	// log.Printf("Server %d-%d got MoveShard request %v", kv.me, kv.gid, args)
	op := Op{}
	op.Type, op.ConfigNum, op.MovingShardNums, op.MovingShardDBs, op.MovingShardClientReqs = ReceiveShard, args.ConfigNum, args.ShardNums, args.ShardDBs, args.ShardClientReqs
	// log.Printf("Server %d-%d committing %s op to all servers", kv.me, kv.gid, op.Type)
	err := kv.commitOpToAllServers(op)
	// log.Printf("Server %d-%d done committing %s op to all servers", kv.me, kv.gid, op.Type)
	// if ok {
	// 	reply.Err = OK
	// } else {
	// 	reply.Err = ErrWrongLeader
	// }
	reply.Err = err

}

// func (kv *ShardKV) checkLeader() {
// 	for {
// 		select {
// 		case isLeader := <-kv.rf.LeaderCh:
// 			// // log.Printf("Server %d-%d received %t from leaderCh", kv.me, kv.gid, isLeader)
// 			kv.mu.Lock()
// 			kv.isLeader = isLeader
// 			kv.mu.Unlock()
// 		}
// 	}
// }

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	// log.Printf("Making Server %d-%d", me, gid)

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	kv.sm = shardmaster.MakeClerk(kv.masters)
	kv.currConfigNum = 0

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.rf = raft.Make(servers, me, kv.persister, kv.applyCh)
	kv.opChans = make(map[int]chan interface{})
	kv.shardedDB = make(map[int]DB)
	kv.clientReqs = make(map[int]ClientReqLog)
	kv.groupReqs = make(map[int]GroupReqLog)
	kv.killCh = make(chan bool, 1)
	kv.shardsOwned = make([]ShardState, shardmaster.NShards)
	// for shard := 0; shard < shardmaster.NShards; shard++ {
	// 	kv.shardsOwned[shard] = VALID
	// 	kv.shardedDB[shard] = DB{}
	// 	kv.clientReqs[shard] = ClientReqLog{}
	// }

	snapshot := kv.persister.ReadSnapshot()
	kv.applySnapshot(snapshot)

	go kv.applyCommitted()
	go kv.syncConfig()
	// go kv.checkLeader()

	return kv
}
