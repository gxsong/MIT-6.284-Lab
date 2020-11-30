package shardmaster

//
// Shardmaster clerk.
//

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
	ck := new(Clerk)
	ck.servers = servers
	ck.leaderID = 0
	ck.clientID = nrand()
	ck.serial = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.serial++
	for {
		// try each known server.
		args, reply := QueryArgs{ck.clientID, ck.serial, num}, QueryReply{}
		ok := ck.servers[ck.leaderID].Call("ShardMaster.Query", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK {
			return reply.Config
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.serial++
	for {
		// try each known server.
		args, reply := JoinArgs{ck.clientID, ck.serial, servers}, JoinReply{}
		ok := ck.servers[ck.leaderID].Call("ShardMaster.Join", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.serial++
	for {
		// try each known server.
		args, reply := LeaveArgs{ck.clientID, ck.serial, gids}, LeaveReply{}
		ok := ck.servers[ck.leaderID].Call("ShardMaster.Leave", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.serial++
	for {
		// try each known server.
		args, reply := MoveArgs{ck.clientID, ck.serial, shard, gid}, MoveReply{}
		ok := ck.servers[ck.leaderID].Call("ShardMaster.Move", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}
