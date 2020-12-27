package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
	"../shardmaster"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	leaderID int
	clientID int64
	serial   int64
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	ck.clientID = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	ck.serial++
	shard := key2shard(key)
	args := GetArgs{ck.clientID, ck.serial, shard, key, GET}

	for {
		gid := ck.config.Shards[shard]
		if servers, present := ck.config.Groups[gid]; present {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				// log.Printf("Client %d Serial %d sending GET request %v", ck.clientID, ck.serial, args)
				ok := srv.Call("ShardKV.Get", &args, &reply)
				// log.Printf("Client %d Serial %d got GET reply %v, ok: %t", ck.clientID, ck.serial, reply, ok)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}

				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(200 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op OpType) {
	ck.serial++
	shard := key2shard(key)
	args := PutAppendArgs{ck.clientID, ck.serial, shard, key, value, op}

	for {
		gid := ck.config.Shards[shard]
		if servers, present := ck.config.Groups[gid]; present {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				// log.Printf("Client %d Serial %d sending PUTAPPEND request %v", ck.clientID, ck.serial, args)
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				// log.Printf("Client %d Serial %d got PUTAPPEND reply %v", ck.clientID, ck.serial, reply)
				if ok && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(200 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
