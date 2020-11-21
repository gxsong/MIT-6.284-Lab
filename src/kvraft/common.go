package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type OpType string

const (
	PUT    OpType = "PUT"
	APPEND OpType = "APPEND"
	GET    OpType = "GET"
)

type Op struct {
	ClientID int64
	Serial   int64
	Type     OpType
	Key      string
	Value    string
}

// Put or Append
type PutAppendArgs struct {
	ClientID int64
	Serial   int64
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
	Key      string
	OpType   OpType
}

type GetReply struct {
	Err   Err
	Value string
}
