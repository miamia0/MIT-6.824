package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"sync"
	"sync/atomic"
)

type Clerk struct {
	mu sync.Mutex

	servers    []*labrpc.ClientEnd
	ckid       int64
	seq        int64
	lastLeader int
	// You will have to modify this struct.
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
	// You'll have to add code here.
	ck.ckid = nrand()
	ck.seq = 0
	ck.lastLeader = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	seq := atomic.AddInt64(&ck.seq, 1)
	args := GetArgs{
		Key:  key,
		Ckid: ck.ckid,
		Seq:  seq,
	}

	for serverID := ck.lastLeader; ; serverID = (serverID + 1) % len(ck.servers) {

		reply := GetReply{}
		success := ck.servers[serverID].Call("RaftKV.Get", &args, &reply)
		if success && !reply.WrongLeader {
			ck.lastLeader = serverID
			return reply.Value
		}
		// You will have to modify this function.
	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	ck.mu.Lock()
	seq := atomic.AddInt64(&ck.seq, 1)
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Ckid:  ck.ckid,
		Seq:   seq,
	}

	ck.mu.Unlock()
	for serverID := ck.lastLeader; ; serverID = (serverID + 1) % len(ck.servers) {
		reply := PutAppendReply{}
		success := ck.servers[serverID].Call("RaftKV.PutAppend", &args, &reply)
		if success && !reply.WrongLeader {
			ck.lastLeader = serverID
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
