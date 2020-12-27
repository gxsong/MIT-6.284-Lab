package shardkv

import (
	"../shardmaster"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongGroup     = "ErrWrongGroup"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrDupRequest     = "ErrDupRequest"
	ErrUpdatingConfig = "ErrUpdatingConfig"
)

type Err string

type OpType string

const (
	PUT          OpType = "PUT"
	APPEND       OpType = "APPEND"
	GET          OpType = "GET"
	NewConfig    OpType = "NewConfig"
	SendShard    OpType = "SendShard"
	ReceiveShard OpType = "ReceiveShard"
)

type ShardState string

const (
	VALID   ShardState = "VALID"
	PENDING ShardState = "PENDING"
	INVALID ShardState = "INVALID"
)

type Op struct {
	// Put/Append/Get Op fields:
	ClientID int64
	Serial   int64
	Type     OpType
	Shard    int
	Key      string
	Value    string
	// NewConfig Op fields:
	ConfigNum int
	Config    shardmaster.Config
	// SendShard Op fields:
	DestGID         int
	MovingShardNums []int
	// ReceiveShard Op fields:
	MovingShardDBs        map[int]DB
	MovingShardClientReqs map[int]ClientReqLog
}

func (op *Op) equal(other interface{}) bool {
	otherOp := other.(Op)
	return op.ClientID == otherOp.ClientID && op.Serial == otherOp.Serial && op.ConfigNum == otherOp.ConfigNum
}

func (op *Op) copy() Op {
	opCopy := Op{}
	opCopy.ClientID,
		opCopy.Serial,
		opCopy.Type,
		opCopy.Shard,
		opCopy.Key,
		opCopy.Value,
		opCopy.ConfigNum,
		opCopy.Config,
		opCopy.DestGID,
		opCopy.MovingShardNums,
		opCopy.MovingShardDBs,
		opCopy.MovingShardClientReqs = op.ClientID,
		op.Serial,
		op.Type,
		op.Shard,
		op.Key,
		op.Value,
		op.ConfigNum,
		copyConfig(op.Config),
		op.DestGID,
		append(opCopy.MovingShardNums, op.MovingShardNums...),
		map[int]DB{},
		map[int]ClientReqLog{}

	for shard, DB := range op.MovingShardDBs {
		opCopy.MovingShardDBs[shard] = copyDBShard(DB)
	}
	for shard, clientReqs := range op.MovingShardClientReqs {
		opCopy.MovingShardClientReqs[shard] = copyClientReqShard(clientReqs)
	}
	return opCopy
}

// Put or Append
type PutAppendArgs struct {
	ClientID int64
	Serial   int64
	Shard    int
	Key      string
	Value    string
	OpType   OpType
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	// For request dupication. See raft extended paper section 8
	ClientID int64
	Serial   int64
	Shard    int
	Key      string
	OpType   OpType
}

type GetReply struct {
	Err   Err
	Value string
}

type MoveShardArgs struct {
	FromGID         int
	ToGID           int
	ConfigNum       int
	ShardNums       []int
	ShardDBs        map[int]DB
	ShardClientReqs map[int]ClientReqLog
}

type MoveShardReply struct {
	Err Err
}

type DB map[string]string
type ClientReqLog map[int64]int64
type GroupReqLog map[int]bool
