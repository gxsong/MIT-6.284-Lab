package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leaderID int
	clientID int64
	serial   int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	// DPrintf("making client")
	ck := new(Clerk)
	ck.servers = servers
	ck.leaderID = 0
	ck.clientID = nrand()
	ck.serial = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.serial++
	for {
		args, reply := GetArgs{ck.clientID, ck.serial, key, GET}, GetReply{}
		DPrintf("[kv][Client] Client making Get request to server %d", ck.leaderID)
		ok := ck.servers[ck.leaderID].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			continue
		}
		DPrintf("[kv][Client] Client got reply %v", reply)
		if reply.Err == OK {
			return reply.Value
		} else if reply.Err == ErrNoKey {
			return ""
		}
		time.Sleep(100 * time.Millisecond)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op OpType) {
	ck.serial++
	for {
		args, reply := PutAppendArgs{ck.clientID, ck.serial, key, value, op}, PutAppendReply{}
		DPrintf("[kv][Client] Client making Get request to server %d", ck.leaderID)
		ok := ck.servers[ck.leaderID].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			continue
		}
		DPrintf("[kv][Client] Client got reply %v from server %d", reply, ck.leaderID)
		if reply.Err == OK {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
