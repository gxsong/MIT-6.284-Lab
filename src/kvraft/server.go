package kvraft

import (
	"sync"
	"sync/atomic"
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
	// 1. send op to raft
	index, _, ok := kv.rf.Start(op)
	if !ok {
		return false
	}
	// wait for the op to get committed
	kv.mu.Lock()
	opChan, ok := kv.opChans[index]
	if !ok {
		opChan = make(chan interface{}, 1)
		kv.opChans[index] = opChan
	}
	kv.mu.Unlock()
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
	DPrintf("[kv][Server] Server %d got PutAppend Request", kv.me)
	op := Op{args.ClientID, args.Serial, args.OpType, args.Key, args.Value}
	ok := kv.handleRequest(op)
	if ok {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) apply(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
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
		DPrintf("[kv][Server] for loop..")
		cmd := <-kv.applyCh
		DPrintf("[kv][Server] Server %d got command %v", kv.me, cmd)
		// update kv state to let handler know which ops are applied
		index, op := cmd.CommandIndex, cmd.Command.(Op)
		// Only execute op if it's serial number is the latest of the client's, for request deduplication
		// Consider the scenario when the leader committed the op but crashed before replying to the client
		// The client will got request timeout and resend the same request to a new leader. The same request will then be applied twice.

		// execute op if it's putappend, let the handler execute get, because it's harder to put err no key back to hander
		if serial, present := kv.clientReqs[op.ClientID]; !present || op.Serial > serial {
			kv.apply(op)
			kv.clientReqs[op.ClientID] = op.Serial
		}
		kv.mu.Lock()
		opChan, present := kv.opChans[index]
		DPrintf("[kv][Server] Server %d sending op to opChan at index %d", kv.me, index)
		if present {
			opChan <- op
		}
		DPrintf("[kv][Server] Server %d end sending op to opChan at index %d", kv.me, index)
		// if not present it means no handlers waiting on that index
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
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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

	// kick off a long-running go routine
	go kv.applyCommitted()

	return kv
}
