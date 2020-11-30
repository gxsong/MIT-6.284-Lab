package shardmaster

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type OpType string

const (
	APPEND OpType = "APPEND" // for new configs made in join(), leave(), and move()
	GET    OpType = "GET"    // for query()
)

type Op struct {
	ClientID  int64
	Serial    int64
	Type      OpType
	ConfigNum int    // key: config number
	Config    Config // value: config
}

func (op *Op) equal(other interface{}) bool {
	otherOp := other.(Op)
	return op.ClientID == otherOp.ClientID && op.Serial == otherOp.Serial
}

type JoinArgs struct {
	ClientID int64
	Serial   int64
	Servers  map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	Err Err
}

type LeaveArgs struct {
	ClientID int64
	Serial   int64
	GIDs     []int
}

type LeaveReply struct {
	Err Err
}

type MoveArgs struct {
	ClientID int64
	Serial   int64
	Shard    int
	GID      int
}

type MoveReply struct {
	Err Err
}

type QueryArgs struct {
	ClientID int64
	Serial   int64
	Num      int // desired config number
}

type QueryReply struct {
	Err    Err
	Config Config
}
