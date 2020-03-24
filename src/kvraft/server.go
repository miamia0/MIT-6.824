package raftkv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string
	Key   string
	Value string
	Ckid  int64
	Seq   int64
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	db     map[string]string
	chMap  map[int]chan Op
	cidSeq map[int64]int64

	persister *raft.Persister
	// Your definitions here.
	Debug bool
}

func (kv *RaftKV) getLock(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, success := kv.chMap[index]
	if success == false {
		kv.chMap[index] = make(chan Op, 1)
	}
	return kv.chMap[index]
}
func (kv *RaftKV) unLockIndex(logIndex int, op Op) {
	ch := kv.getLock(logIndex)
	ch <- op
}
func (kv *RaftKV) lockIndex(logIndex int) Op {
	ch := kv.getLock(logIndex)
	select {
	case op := <-ch:
		return op
	case <-time.After(time.Second):
		return Op{}
	}
}
func equalOp(a Op, b Op) bool {
	return a.Key == b.Key && a.Value == b.Value && a.Type == b.Type
}
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	originOp := Op{Type: args.Op,
		Key:   args.Key,
		Value: args.Value,
		Ckid:  args.Ckid,
		Seq:   args.Seq,
	}
	_, isLeader := kv.rf.GetState()

	reply.WrongLeader = true

	if !isLeader {
		return
	}
	if kv.Debug {

		fmt.Println(kv.me, " start a originOp ", originOp)
	}
	logIndex, _, isLeader := kv.rf.Start(originOp)
	if !isLeader {
		return
	}
	op := kv.lockIndex(logIndex)

	if equalOp(originOp, op) {
		reply.WrongLeader = false
		reply.Value = kv.db[op.Key]

		if kv.Debug {

			fmt.Println("Get kv[", op.Key, "] = ", reply.Value)
		}
		return
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	originOp := Op{Type: args.Op,
		Key:   args.Key,
		Value: args.Value,
		Ckid:  args.Ckid,
		Seq:   args.Seq,
	}

	_, isLeader := kv.rf.GetState()
	reply.WrongLeader = true

	if !isLeader {
		return
	}
	logIndex, _, isLeader := kv.rf.Start(originOp)
	if !isLeader {
		return
	}
	op := kv.lockIndex(logIndex)

	if equalOp(originOp, op) {
		reply.WrongLeader = false
		return
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//

func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	kv.rf.Debug = false
	kv.Debug = false
	// Your code here, if desired.
}
func (kv *RaftKV) checkNeedSnapShot() bool {
	if kv.maxraftstate < kv.persister.RaftStateSize() {
		return true
	}
	return false
}

func (kv *RaftKV) saveNewSnapShot(index int) bool {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.db)
	e.Encode(kv.cidSeq)

	if kv.Debug {
		fmt.Println(kv.me, " server saveNewSnapShot for index ", index)
	}
	data := w.Bytes()
	kv.rf.SaveNewSnapShotRaft(index, data)

	return false
}
func (kv *RaftKV) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	kv.mu.Lock()
	d.Decode(&kv.db)
	d.Decode(&kv.cidSeq)

	kv.mu.Unlock()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.persister = persister
	kv.db = make(map[string]string)
	kv.chMap = make(map[int]chan Op)

	kv.cidSeq = make(map[int64]int64)

	kv.Debug = false
	kv.rf.Debug = false
	go func() {
		for apply := range kv.applyCh {
			if apply.UseSnapshot {
				kv.readPersist(apply.Snapshot)
				continue
			}
			op := apply.Command.(Op)
			if maxSeq, success := kv.cidSeq[op.Ckid]; !success || op.Seq > maxSeq {
				kv.mu.Lock()
				if op.Type == "Put" {
					kv.db[op.Key] = op.Value
				}
				if op.Type == "Append" {
					kv.db[op.Key] += op.Value
				}
				kv.cidSeq[op.Ckid] = op.Seq

				kv.mu.Unlock()

			}
			kv.unLockIndex(apply.Index, op)
			if kv.maxraftstate != -1 && kv.checkNeedSnapShot() {
				kv.saveNewSnapShot(apply.Index)
			}
		}
	}()
	kv.readPersist(persister.ReadSnapshot())
	return kv
}
