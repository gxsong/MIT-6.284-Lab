package kvraft

import (
	"bytes"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		DPrintf(format, a...)
	}
	return
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	opChans    map[int]chan interface{} // index to Op map
	store      map[string]string
	clientReqs map[int64]int64 // stores the lates serial number of each client

	persister    *raft.Persister
	maxraftstate int // snapshot if log grows this big
	killCh       chan bool
}

/**
* Consider schenarios:
* 1. Leader raft functions as usual, client send request:
* 		raft commits the `op` at `index` returned by start(), `op` is sent back to `opChan` normally
* 2. Leader raft disconnected from network, ops sent from clients won't be committed:
* 		kv has an `opChan` at `index` but never hear back from `opChan`, timeout and return
* 3. Leader reconnected as follower, a lot of logs from other rafts came in via applyCh:
* 		those logs will be applied, but doesn't belong to any requests this server recieved
* 		if there's an index overlap (uncommited log from #2 already existing), the committed op will be passed back to opChan and ignored
 */
func (kv *KVServer) handleRequest(op Op) bool {
	DPrintf("Server %d Handle request", kv.me)
	// 1. send op to raft
	index, _, ok := kv.rf.Start(op)
	if !ok {
		return false
	}
	// wait for the op to get committed
	// TODO: locking issue?
	DPrintf("Server %d Handle request before locking", kv.me)
	kv.mu.Lock()
	opChan, ok := kv.opChans[index]
	if !ok {
		opChan = make(chan interface{}, 1)
		kv.opChans[index] = opChan
	}
	kv.mu.Unlock()
	DPrintf("Server %d before Select", kv.me)
	// 2. wait for raft to commit log and execute command
	// 		- if the exact op is commited and applied, return values
	//		- if the wrong op is committed, return error wrong leader
	// 		- if never commited, after a timeout, return error wrong leader (raft is dead or stale)
	select {
	case appliedOp := <-opChan:
		DPrintf("[kv][Server] Server %d applied op %v, op sent:%v", kv.me, appliedOp, op)
		if op == appliedOp {
			return true
		} else {
			return false
		}
	case <-time.After(1000 * time.Millisecond):
		DPrintf("[kv][Server] Server %d Request timed out %v", kv.me, op)
		return false
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("[kv][Server] Server %d got Get Request", kv.me)
	op := Op{args.ClientID, args.Serial, args.OpType, args.Key, ""}
	ok := kv.handleRequest(op)
	if ok {
		kv.mu.Lock()
		value, present := kv.store[op.Key]
		if !present {
			reply.Err = ErrNoKey
		} else {
			reply.Value = value
			reply.Err = OK
		}
		kv.mu.Unlock()
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("[kv][Server] Server %d got PutAppend Request {%s %s}", kv.me, args.Key, args.Value)
	op := Op{args.ClientID, args.Serial, args.OpType, args.Key, args.Value}
	ok := kv.handleRequest(op)
	if ok {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) apply(op Op) {
	if op.Type == PUT {
		kv.store[op.Key] = op.Value
	} else if op.Type == APPEND {
		val, present := kv.store[op.Key]
		if !present {
			val = ""
		}
		kv.store[op.Key] = val + op.Value
	}
}

// listening kv.applyCh for committed logs from raft
// apply ops from commited log (NOTE that the op doesn't have to belong to any request received by this server. it could be from some previous leader)
// let handler know that log has been applied
func (kv *KVServer) applyCommitted() {
	for {
		select {
		case <-kv.killCh:
			// DPrintf("Server %d killed", kv.me)
			return
		default:
			// DPrintf("Server %d still alive", kv.me)
		}
		cmd := <-kv.applyCh

		// apply snapshot
		if cmd.IsSnapshot {
			DPrintf("[kv][Server] Server %d got snapshot", kv.me)
			kv.applySnapshot(cmd.Command.([]byte))
			continue
		}

		DPrintf("[kv][Server] Server %d got command %v", kv.me, cmd)

		// update kv state to let handler know which ops are applied
		index, op := cmd.CommandIndex, cmd.Command.(Op)
		// Only execute op if it's serial number is the latest of the client's, for request deduplication
		// Consider the scenario when the leader committed the op but crashed before replying to the client
		// The client will got request timeout and resend the same request to a new leader. The same request will then be applied twice.

		// execute op if it's putappend, let the handler execute get, because it's harder to put err no key back to hander
		kv.mu.Lock()
		if serial, present := kv.clientReqs[op.ClientID]; !present || op.Serial > serial {
			kv.apply(op)
			kv.clientReqs[op.ClientID] = op.Serial
		}

		opChan, present := kv.opChans[index]

		if present {
			DPrintf("[kv][Server] Server %d sending op to opChan at index %d", kv.me, index)
			opChan <- op
			DPrintf("[kv][Server] Server %d end sending op to opChan at index %d", kv.me, index)
		}

		// if not present it means no handlers waiting on that index

		kv.checkSizeAndPersistSnapshot(index)
		kv.mu.Unlock()

	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	// atomic.StoreInt32(&kv.dead, 1)
	// DPrintf("Kill() at Server %d", kv.me)
	kv.rf.Kill()
	select {
	case <-kv.killCh: //if already set, consume it then resent to avoid block
		// return
	default:
	}
	// DPrintf("before killCh at Server %d", kv.me)
	kv.killCh <- true
	// DPrintf("killing server %d", kv.me)
}

func (kv *KVServer) killed() bool {
	// z := atomic.LoadInt32(&kv.dead)
	return false
}

func (kv *KVServer) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.clientReqs)
	e.Encode(kv.store)
	return w.Bytes()
}

func (kv *KVServer) decodeSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.clientReqs)
	d.Decode(&kv.store)
}

// Check if raft state size is larger than maxraftstate
// if so, send a snapshot to the raft

// kv staste to save in snapshot:
// kv.store, kv.clientReqs
func (kv *KVServer) checkSizeAndPersistSnapshot(index int) {
	if kv.maxraftstate == -1 {
		return
	}
	if kv.persister.RaftStateSize()/10 >= kv.maxraftstate/10 { // take snapshot when appraoching limit
		snapshot := kv.encodeSnapshot()
		DPrintf("[kv][Server] server %d applying snapshot %v at index %d", kv.me, snapshot, index)
		// must spawn another goroutine, otherwise server will be blocked waiting on raft's lock
		go kv.rf.CompactLogAndSaveSnapshot(snapshot, index)
	}
}

// apply snapshot to kv's state
func (kv *KVServer) applySnapshot(snapshot []byte) {
	// snapshot := kv.persister.ReadSnapshot()
	DPrintf("[kv][Server] Serve %d applying Snapshot %v", kv.me, snapshot)
	kv.mu.Lock()
	kv.decodeSnapshot(snapshot)
	kv.mu.Unlock()
	DPrintf("[kv][Server] Serve %d done applying Snapshot", kv.me)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.rf = raft.Make(servers, me, kv.persister, kv.applyCh)
	kv.opChans = make(map[int]chan interface{})
	kv.store = make(map[string]string)
	kv.clientReqs = make(map[int64]int64)
	kv.killCh = make(chan bool, 1)

	snapshot := kv.persister.ReadSnapshot()
	kv.applySnapshot(snapshot)

	go kv.applyCommitted()

	return kv
}
