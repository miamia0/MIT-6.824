package shardkv

import "shardmaster"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CkConfigNum int
	Ckid        int64
	Seq         int64
	Shard       int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	CkConfigNum int

	Ckid  int64
	Seq   int64
	Shard int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type InstallShardArgs struct {
	Data      map[string]string
	CKid2Seq  map[int64]int64
	Shard     int
	ConfigNum int
	Ckid      int64
	Seq       int64
}
type InstallShardReply struct {
	CKid2Seq    map[int64]int64
	WrongLeader bool
	Success     bool
}
type DeleteShardArgs struct {
	Shard     int
	ConfigNum int
}
type DeleteShardReply struct {
	Success bool
}

type ExcuteInstallShardArgs struct {
	Shard     int
	ConfigNum int
	CKid2Seq  map[int64]int64
	Data      map[string]string
}
type ExcuteDeleteShardArgs struct {
	Shard     int
	ConfigNum int
}
type ExcuteNewConfigArgs struct {
	Config shardmaster.Config
}
type ExcuteUpdateCidSeqArgs struct {
	CKid2Seq map[int64]int64
}
type ExcuteGetArgs struct {
	Shard int
	Key   string
	Value string
}
type ExcutePutAppendArgs struct {
	Shard int
	Key   string
	Value string
}
