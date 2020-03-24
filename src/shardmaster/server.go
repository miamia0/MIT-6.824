package shardmaster

import (
	"encoding/gob"
	"labrpc"
	"raft"
	"sync"
	"time"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	Debug   bool
	// Your data here.

	configs []Config // indexed by config num

	chMap  map[int]chan Op
	cidSeq map[int64]int64

	gidShardMap map[int][]int
	groupCnt    int
}

type Op struct {
	// Your data here.
	Type string
	Ckid int64
	Seq  int64
	Args interface{}
}

func (sm *ShardMaster) getLock(index int) chan Op {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	_, success := sm.chMap[index]
	if success == false {
		sm.chMap[index] = make(chan Op, 1)
	}
	return sm.chMap[index]
}
func (sm *ShardMaster) unLockIndex(logIndex int, op Op) {
	ch := sm.getLock(logIndex)
	ch <- op
}
func (sm *ShardMaster) lockIndex(logIndex int) Op {
	ch := sm.getLock(logIndex)
	select {
	case op := <-ch:
		return op
	case <-time.After(time.Second):
		return Op{}
	}
}
func equalOp(a Op, b Op) bool {
	return a.Type == b.Type && a.Ckid == b.Ckid && a.Seq == b.Seq
}
func (sm *ShardMaster) syncWithRaft(originOp Op) (Op, bool) {
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		return Op{}, false
	}
	logIndex, _, isLeader := sm.rf.Start(originOp)
	if !isLeader {
		return Op{}, false
	}
	op := sm.lockIndex(logIndex)
	return op, true
}
func (sm *ShardMaster) newConfig() Config {
	cfg := sm.configs[len(sm.configs)-1]
	newCfg := Config{
		Num:    cfg.Num + 1,
		Shards: cfg.Shards,
		Groups: make(map[int][]string),
	}
	for gid, servers := range cfg.Groups {
		newCfg.Groups[gid] = append(newCfg.Groups[gid], servers...)
	}
	return newCfg
}
func (sm *ShardMaster) changeGid(cfg *Config, sid, gid int) {
	cfg.Shards[sid] = gid
}
func (sm *ShardMaster) popMaxShardGid() int {
	maxLen, maxGid := -1, 0
	for gid, shards := range sm.gidShardMap {
		if len(shards) > maxLen {
			maxLen = len(shards)
			maxGid = gid
		}
	}
	return maxGid
}
func (sm *ShardMaster) popMinShardGid(changedGid int) int {
	minLen, minGid := 10000000, 0
	for gid, shards := range sm.gidShardMap {
		if gid != changedGid && len(shards) < minLen {
			minLen = len(shards)
			minGid = gid
		}
	}
	return minGid
}
func (sm *ShardMaster) isSharded(cfg *Config, changedGid int) bool {
	sharded := true
	for sid, gid := range cfg.Shards {
		if gid == 0 {
			sharded = false
			sm.changeGid(cfg, sid, changedGid)
			sm.gidShardMap[changedGid] = append(sm.gidShardMap[changedGid], sid)
		}
	}
	return sharded

}

//todo堆实现
func (sm *ShardMaster) general_rebanlance(cfg *Config) {
	for {
		maxGid := sm.popMaxShardGid()
		minGid := sm.popMinShardGid(-1)
		if maxGid > minGid+1 {
			sid := sm.gidShardMap[maxGid][0]
			cfg.Shards[sid] = minGid
			sm.gidShardMap[maxGid] = sm.gidShardMap[maxGid][1:]
			sm.gidShardMap[minGid] = append(sm.gidShardMap[minGid], sid)
		} else {
			break
		}
	}
}

func (sm *ShardMaster) rebanlance(cfg *Config, phase string, changedGid int) {
	shardCnt := len(cfg.Shards)
	if sm.groupCnt == 0 {
		for sid, _ := range cfg.Shards {
			cfg.Shards[sid] = 0
		}
		return
	}
	if sm.isSharded(cfg, changedGid) == false {
		return
	}

	mxShardCnt := (shardCnt + sm.groupCnt - 1) / sm.groupCnt

	if phase == "Join" {
		for i := 0; i < mxShardCnt; i++ {
			Gid := sm.popMaxShardGid() //todo 堆
			sid := sm.gidShardMap[Gid][0]
			cfg.Shards[sid] = changedGid
			sm.gidShardMap[Gid] = sm.gidShardMap[Gid][1:]
			sm.gidShardMap[changedGid] = append(sm.gidShardMap[changedGid], sid)
		}
	}
	if phase == "Leave" {
		for _, sid := range sm.gidShardMap[changedGid] {
			Gid := sm.popMinShardGid(changedGid) //todo 堆
			cfg.Shards[sid] = Gid
			sm.gidShardMap[Gid] = append(sm.gidShardMap[Gid], sid)
		}
		delete(sm.gidShardMap, changedGid)
	}
	///	sm.general_rebanlance(cfg)

}
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	originOp := Op{Type: "Join", Ckid: args.Ckid, Seq: args.Seq, Args: *args}
	reply.WrongLeader = true

	op, success := sm.syncWithRaft(originOp)
	if success && equalOp(originOp, op) {
		reply.WrongLeader = false

		return
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.

	originOp := Op{Type: "Leave", Ckid: args.Ckid, Seq: args.Seq, Args: *args}
	reply.WrongLeader = true
	op, success := sm.syncWithRaft(originOp)
	if success && equalOp(originOp, op) {
		reply.WrongLeader = false

		return
	}

}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

	originOp := Op{Type: "Move", Ckid: args.Ckid, Seq: args.Seq, Args: *args}
	reply.WrongLeader = true
	op, success := sm.syncWithRaft(originOp)
	if success && equalOp(originOp, op) {
		reply.WrongLeader = false

		return
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	originOp := Op{Type: "Query", Ckid: args.Ckid, Seq: args.Seq}
	reply.WrongLeader = true
	op, success := sm.syncWithRaft(originOp)
	if success && equalOp(originOp, op) {
		reply.WrongLeader = false
		if args.Num >= 0 && args.Num < len(sm.configs) {
			reply.Config = sm.configs[args.Num]
		} else {
			reply.Config = sm.configs[len(sm.configs)-1]
		}
		return
	}

}

func (sm *ShardMaster) excuteOp(op Op) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	newCfg := sm.newConfig()
	Args := op.Args
	switch op.Type {
	case "Join":
		{
			args := Args.(JoinArgs)
			for gid, servers := range args.Servers {
				if _, success := newCfg.Groups[gid]; !success {
					sm.groupCnt++
					sm.rebanlance(&newCfg, "Join", gid)
				}
				newServers := make([]string, len(servers))
				copy(newServers, servers)
				newCfg.Groups[gid] = newServers
			}
		}
	case "Leave":
		{
			args := Args.(LeaveArgs)

			for _, gid := range args.GIDs {
				if _, success := newCfg.Groups[gid]; success {
					sm.groupCnt--
					sm.rebanlance(&newCfg, "Leave", gid)
				}
				delete(newCfg.Groups, gid)
			}
		}
	case "Move":
		{
			args := Args.(MoveArgs)
			if _, success := newCfg.Groups[args.GID]; success {
				newCfg.Shards[args.Shard] = args.GID
			} else {
				delete(newCfg.Groups, args.GID)
				return
			}
		}
	}
	sm.configs = append(sm.configs, newCfg)

}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	sm.Debug = false
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)

	sm.configs[0].Groups = map[int][]string{}
	gob.Register(Op{})
	gob.Register(JoinArgs{})
	gob.Register(MoveArgs{})
	gob.Register(LeaveArgs{})

	sm.groupCnt = 0
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.chMap = make(map[int]chan Op)
	sm.cidSeq = make(map[int64]int64)
	sm.gidShardMap = make(map[int][]int)
	sm.Debug = false
	sm.rf.Debug = false

	// Your code here.
	go func() {
		for apply := range sm.applyCh {
			op := apply.Command.(Op)
			if maxSeq, success := sm.cidSeq[op.Ckid]; !success || op.Seq > maxSeq {
				sm.cidSeq[op.Ckid] = op.Seq
				if op.Type != "Query" {
					sm.excuteOp(op)
				}
			}
			sm.unLockIndex(apply.Index, op)

		}
	}()
	return sm
}
