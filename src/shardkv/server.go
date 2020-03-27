package shardkv

// import "shardmaster"
import (
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"raft"
	"shardmaster"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type        string
	Ckid        int64
	Seq         int64
	CkConfigNum int
	Err         Err
	Args        interface{}
}
type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	db        map[int]map[string]string
	chMap     map[int]chan Op
	chWaitMap map[int]chan bool
	cidSeq    map[int64]int64

	Ckid      int64
	seq       int64
	mck       *shardmaster.Clerk
	config    shardmaster.Config
	persister *raft.Persister
	// Your definitions here.
	configNumDeleteShard map[int][]int                     //configNum_sidlist
	dbInstallShard       map[int]map[int]map[string]string //sid_k_v

	isVailedShard       map[int]bool
	wantShardConfig     map[int]int
	needSendShardConfig map[int]int
	historyConfig       map[int]shardmaster.Config
	Ldebug              bool
	Debug               bool
}

func (kv *ShardKV) checkNeedSnapShot() bool {
	if kv.maxraftstate < kv.persister.RaftStateSize() {
		return true
	}
	return false
}

func (kv *ShardKV) saveNewSnapShot(index int) bool {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	e.Encode(kv.Ckid)
	e.Encode(kv.seq)
	e.Encode(kv.db)
	e.Encode(kv.cidSeq)
	e.Encode(kv.config)

	e.Encode(kv.dbInstallShard)
	e.Encode(kv.isVailedShard)
	e.Encode(kv.wantShardConfig)
	e.Encode(kv.needSendShardConfig)
	e.Encode(kv.historyConfig)

	data := w.Bytes()
	kv.rf.SaveNewSnapShotRaft(index, data)

	return false
}
func (kv *ShardKV) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&kv.Ckid)
	d.Decode(&kv.seq)
	d.Decode(&kv.db)
	d.Decode(&kv.cidSeq)
	d.Decode(&kv.config)

	d.Decode(&kv.dbInstallShard)
	d.Decode(&kv.isVailedShard)
	d.Decode(&kv.wantShardConfig)
	d.Decode(&kv.needSendShardConfig)
	d.Decode(&kv.historyConfig)

}

func (kv *ShardKV) getLock(index int) chan Op {
	//	fmt.Println("getLock lock mu")
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, success := kv.chMap[index]; success == false {
		kv.chMap[index] = make(chan Op, 1)
	}
	//	fmt.Println("getLock release mu")

	return kv.chMap[index]
}
func (kv *ShardKV) getWaitLock(index int) chan bool {
	//	fmt.Println("getLock lock mu")
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, success := kv.chWaitMap[index]; success == false {
		kv.chWaitMap[index] = make(chan bool, 1)
	}
	//	fmt.Println("getLock release mu")

	return kv.chWaitMap[index]
}
func (kv *ShardKV) unLockIndex(logIndex int, op Op) bool {
	//	fmt.Println("index", logIndex, "unlock ")
	ch := kv.getLock(logIndex)
	ch <- op
	waitLock := kv.getWaitLock(logIndex)
	select {
	case <-waitLock:
		return true
	default:
		return false

	}
}
func (kv *ShardKV) lockIndex(logIndex int) Op {
	//	fmt.Println("order to index", logIndex, "may outTime")

	ch := kv.getLock(logIndex)
	//	fmt.Println("index", logIndex, "may outTime")
	kv.getWaitLock(logIndex) <- true
	select {
	case op := <-ch:
		return op
	case <-time.After(time.Second * 10):
		<-kv.getWaitLock(logIndex)
		if kv.Debug {
			fmt.Println(kv.gid, kv.me, "index", logIndex, "outof Date")
		}
		return Op{}
	}
}
func equalOp(a Op, b Op) bool {
	return a.Ckid == b.Ckid && a.Seq == b.Seq
}

//Get RPC
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	reply.WrongLeader = true
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}
	if kv.Debug {
		fmt.Println("gid:", kv.gid, kv.me, "recive  a Get RPC", args)
	}
	originOp := Op{
		Type:        "Get",
		Ckid:        args.Ckid,
		Seq:         args.Seq,
		CkConfigNum: args.CkConfigNum,
		Args:        ExcuteGetArgs{Key: args.Key, Shard: args.Shard},
	}
	if kv.Debug {
		fmt.Println(kv.me, "excute Get ", args)
	}

	logIndex, _, isLeader := kv.rf.Start(originOp)
	if !isLeader {
		return
	}
	op := kv.lockIndex(logIndex)
	kv.mu.Lock()

	if equalOp(originOp, op) {
		reply.WrongLeader = false
		reply.Value = op.Args.(ExcuteGetArgs).Value
		if op.Err == "seq" {
			op.Err = OK
		}
		reply.Err = op.Err
	}
	kv.mu.Unlock()

}

//PutAppend RPC
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.WrongLeader = true
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}
	originOp := Op{
		Type:        args.Op,
		Ckid:        args.Ckid,
		Seq:         args.Seq,
		Args:        ExcutePutAppendArgs{Key: args.Key, Shard: args.Shard, Value: args.Value},
		CkConfigNum: args.CkConfigNum,
	}
	if kv.Debug {
		fmt.Println("gid,", kv.gid, kv.me, "recive  a PutAppend RPC", args)
	}

	logIndex, _, isLeader := kv.rf.Start(originOp)
	if !isLeader {
		return
	}
	op := kv.lockIndex(logIndex)
	kv.mu.Lock()

	if equalOp(originOp, op) {
		if kv.Debug {
			fmt.Println("gid,", kv.gid, kv.me, "ended a PutAppend RPC", args, "op.Err", op, "originOp", originOp)
		}
		if op.Err == "seq" {
			op.Err = OK
		}
		reply.WrongLeader = false
		reply.Err = op.Err
	}
	kv.mu.Unlock()

}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//

func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	kv.Debug = false
	// Your code here, if desired.
}

//InstallShard RPC
func (kv *ShardKV) InstallShard(args *InstallShardArgs, reply *InstallShardReply) {
	//todo
	reply.WrongLeader = true
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}
	if kv.Debug {
		fmt.Println("gid,", kv.gid, kv.me, "recive  a installShard RPC", args)
	}

	originOp := Op{
		Type: "InstallShard",
		Ckid: args.Ckid,
		Seq:  args.Seq,
		Args: ExcuteInstallShardArgs{
			Shard:     args.Shard,
			Data:      args.Data,
			ConfigNum: args.ConfigNum,
			CKid2Seq:  args.CKid2Seq,
		},
	}
	logIndex, _, isLeader := kv.rf.Start(originOp)
	if !isLeader {
		return
	}
	if kv.Debug {

		fmt.Println("gid,", kv.gid, kv.me, "wait for   a installShard RPC", args, "logindex:", logIndex)
	}
	op := kv.lockIndex(logIndex)
	if kv.Debug {

		fmt.Println("gid,", kv.gid, kv.me, "end  a installShard RPC", args)
	}
	if equalOp(originOp, op) {
		if kv.Debug {
			fmt.Println("gid,", kv.gid, kv.me, "excute  a installShard RPC", args, "success")
		}
		reply.WrongLeader = false
		return
	}
}
func (kv *ShardKV) DeleteShard(args *DeleteShardArgs, reply *DeleteShardReply) {
	reply.Success = false
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}
	originOp := Op{
		Type: "DeleteShard",
		Ckid: kv.Ckid,
		Seq:  atomic.AddInt64(&kv.seq, 1),
		Args: ExcuteDeleteShardArgs{
			ConfigNum: args.ConfigNum,
			Shard:     args.Shard,
		},
	}
	logIndex, _, isLeader := kv.rf.Start(originOp)
	if !isLeader {
		return
	}
	op := kv.lockIndex(logIndex)
	if equalOp(originOp, op) {
		reply.Success = true
		return
	}
}
func (kv *ShardKV) sendInstallShard(server string, args *InstallShardArgs, reply *InstallShardReply) bool {
	srv := kv.make_end(server)
	ok := srv.Call("ShardKV.InstallShard", args, reply)
	return ok
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//

func (kv *ShardKV) copyConfig(toConfig, newConfig *shardmaster.Config) {
	for i := range toConfig.Shards {
		toConfig.Shards[i] = newConfig.Shards[i]
	}
	toConfig.Num = newConfig.Num
	toConfig.Groups = make(map[int][]string)
	for gid, servers := range newConfig.Groups {
		toConfig.Groups[gid] = append(toConfig.Groups[gid], servers...)
	}
}

func (kv *ShardKV) sendShardToGroup(servers []string, args *InstallShardArgs) {

	index := 0
	serverLen := len(servers)

	for {
		reply := InstallShardReply{}
		success := kv.sendInstallShard(servers[index], args, &reply)
		if success && !reply.WrongLeader {
			//		kv.UpdateCidSeq(reply.CKid2Seq)
			return
		}
		index = (index + 1) % serverLen
	}
}

//updateConfigShard excute when config changed
func (kv *ShardKV) updateConfigShard(newConfig shardmaster.Config) {
	_, isLeader := kv.rf.GetState()

	if !isLeader {
		return
	}

	if kv.Debug {
		fmt.Println("==========================")
		fmt.Println(kv.gid, kv.me, "update config")
		fmt.Println("now config is ", kv.config)
		fmt.Println("new config is ", newConfig)
		fmt.Println("==========================")

	}
	go kv.NewConfig(newConfig)
	if kv.Debug {
		fmt.Println("end the updateConfigShard")
	}
	return
}

func (kv *ShardKV) NewConfig(Config shardmaster.Config) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}
	originOp := Op{
		Type: "NewConfig",
		Ckid: kv.Ckid,
		Seq:  atomic.AddInt64(&kv.seq, 1),
		Args: ExcuteNewConfigArgs{Config: Config},
	}
	logIndex, _, isLeader := kv.rf.Start(originOp)
	if !isLeader {
		return
	}
	op := kv.lockIndex(logIndex)
	if equalOp(originOp, op) {
		return
	}
}
func (kv *ShardKV) excuteNewConfig(op ExcuteNewConfigArgs) {

	if op.Config.Num <= kv.config.Num {
		return
	}
	if kv.Debug {
		fmt.Println("==========================")
		fmt.Println(kv.gid, kv.me, "excuteRestoreShardFromConfigNum")
		fmt.Println("op.Config:", op.Config)
		fmt.Println("preConfig:", kv.config)
		fmt.Println("===========================")
	}

	newConfig := op.Config
	//大人，时代变了。
	for Sid, Gid := range newConfig.Shards {
		if Gid == kv.gid && kv.config.Shards[Sid] != kv.gid { //need new
			if kv.config.Shards[Sid] == 0 {
				kv.db[Sid] = make(map[string]string)
				kv.isVailedShard[Sid] = true
			} else if _, success := kv.dbInstallShard[Sid][newConfig.Num]; success {
				kv.db[Sid] = DeepCopyShardMap(kv.dbInstallShard[Sid][newConfig.Num])
				for _ConfigNum, _ := range kv.dbInstallShard[Sid] {
					if _ConfigNum <= newConfig.Num {
						delete(kv.dbInstallShard[Sid], newConfig.Num)
					}
				}
				//kv.dbInstallShard[newConfig.Num][Sid] = make(map[string]string)
				kv.isVailedShard[Sid] = true
			} else {
				kv.isVailedShard[Sid] = false
				kv.wantShardConfig[Sid] = newConfig.Num
			}
		} else if Gid != kv.gid && kv.config.Shards[Sid] == kv.gid {

			if kv.isVailedShard[Sid] == true {
				servers := newConfig.Groups[Gid]
				args := &InstallShardArgs{
					Data:      DeepCopyShardMap(kv.db[Sid]),
					CKid2Seq:  kv.cidSeq,
					Shard:     Sid,
					ConfigNum: newConfig.Num,
					Ckid:      kv.Ckid,
					Seq:       atomic.AddInt64(&kv.seq, 1),
				}
				go kv.sendShardToGroup(servers, args)
			} else {
				kv.needSendShardConfig[Sid] = newConfig.Num
			}

			/*	argsDelete := &DeleteShardArgs{
					Shard:     Sid,
					ConfigNum: newConfig.Num,
				}
				reply := DeleteShardReply{}
					go kv.DeleteShard(argsDelete, &reply)
			*/
			delete(kv.db, Sid)
			for _ConfigNum, _ := range kv.dbInstallShard[Sid] {
				if _ConfigNum <= newConfig.Num {
					delete(kv.dbInstallShard[Sid], newConfig.Num)
				}

			}
			kv.isVailedShard[Sid] = false

		} else if Gid == kv.gid && kv.config.Shards[Sid] == kv.gid {
			//not changed
		} else {
			for _ConfigNum, _ := range kv.dbInstallShard[Sid] {
				if _ConfigNum <= newConfig.Num {
					delete(kv.dbInstallShard[Sid], newConfig.Num)
				}
			}
			kv.isVailedShard[Sid] = false
		}
	}
	kv.historyConfig[newConfig.Num] = newConfig
	kv.copyConfig(&kv.config, &newConfig)
	if kv.Debug {
		fmt.Println(kv.gid, kv.me, "newConfig:", kv.config)
		fmt.Println("New db", kv.db)
		fmt.Println("==========================")
	}
}
func (kv *ShardKV) excuteUpdateCidSeq(op ExcuteUpdateCidSeqArgs) {
	ckid2eq := op.CKid2Seq
	for ckid, seq := range ckid2eq {
		kv.cidSeq[ckid] = Max(kv.cidSeq[ckid], seq)
	}
}
func (kv *ShardKV) excuteDeleteShard(op ExcuteDeleteShardArgs) {
	if op.ConfigNum < kv.config.Num {
		return
	}
	sid := op.Shard
	configNum := op.ConfigNum
	if kv.Debug {
		fmt.Println("==========================")
		fmt.Println(kv.gid, kv.me, "excuteDeleteShard", "sid:", sid, "configNum:", configNum)
		fmt.Println("==========================")
	}
	delete(kv.db, sid)
	//kv.configNumDeleteShard[configNum] = append(kv.configNumDeleteShard[configNum], sid)
}
func Max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
func DeepCopyShardMap(shardMap map[string]string) map[string]string {
	newMap := make(map[string]string)

	for k, v := range shardMap {
		newMap[k] = v
	}

	return newMap
}
func (kv *ShardKV) excuteInstallShard(op ExcuteInstallShardArgs) {

	sid := op.Shard
	configNum := op.ConfigNum
	ckid2eq := op.CKid2Seq
	data := op.Data

	for ckid, seq := range ckid2eq {
		kv.cidSeq[ckid] = Max(kv.cidSeq[ckid], seq)
	}
	if op.ConfigNum < kv.wantShardConfig[sid] {
		if kv.Debug {
			fmt.Println("..............")
			fmt.Println(kv.gid, kv.me, "excuteInstallShard Failed....", "sid:", sid, "configNum:", configNum)
			fmt.Println("..............")
		}
		return
	}
	if configNum < kv.needSendShardConfig[sid] {
		needSendConfigNum := kv.needSendShardConfig[sid]
		needSendConfig := kv.historyConfig[needSendConfigNum]
		gid := needSendConfig.Shards[sid]
		servers := needSendConfig.Groups[gid]
		args := &InstallShardArgs{
			Data:      DeepCopyShardMap(data),
			CKid2Seq:  kv.cidSeq,
			Shard:     sid,
			ConfigNum: needSendConfig.Num,
			Ckid:      kv.Ckid,
			Seq:       atomic.AddInt64(&kv.seq, 1),
		}
		kv.needSendShardConfig[sid] = 0
		go kv.sendShardToGroup(servers, args)
	}

	if configNum == kv.wantShardConfig[sid] && kv.isVailedShard[sid] == false {
		if kv.config.Shards[sid] == kv.gid {
			kv.db[sid] = DeepCopyShardMap(data)
			kv.isVailedShard[sid] = true
		} else {

		}
	} else if configNum > kv.wantShardConfig[sid] {
		if _, success := kv.dbInstallShard[sid]; !success {
			kv.dbInstallShard[sid] = make(map[int]map[string]string)
		}
		kv.dbInstallShard[sid][configNum] = DeepCopyShardMap(data)
	}
	if kv.Debug {
		fmt.Println("==========================")
		fmt.Println(kv.gid, kv.me, "excuteInstallShard", "sid:", sid, "configNum:", configNum)
		fmt.Println("new ck2seq:", kv.cidSeq)

		fmt.Println("==========================")
	}
}

//---config store
/*
func (kv *ShardKV) ReStoreWantUpateConfig(newConfig shardmaster.Config) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}
	originOp := Op{
		Type:   "RetoreConfig",
		Ckid:   kv.Ckid,
		Seq:    atomic.AddInt64(&kv.seq, 1),
		Config: newConfig,
	}
	logIndex, _, isLeader := kv.rf.Start(originOp)
	if !isLeader {
		return
	}
	op := kv.lockIndex(logIndex)

	if equalOp(originOp, op) {
		return
	}
}

func (kv *ShardKV) excuteReStoreWantUpateConfig(op Op) {
	config := op.Config
	if config.Num != kv.getLastResToredConfigNum()+1 {
		return
	}
	kv.reStoreConfig = append(kv.reStoreConfig, config)
}
func (kv *ShardKV) getLastResToredConfigNum() int {
	return kv.reStoreConfig[len(kv.reStoreConfig)-1].Num
}
func (kv *ShardKV) updateConfigShard() {
	for {
		<-kv.newConfig
		kv.ReStoreWantUpateConfig(newConfig)
		kv.updateConfigShard(kv.reStoreConfig[len(kv.reStoreConfig)-1])
		time.Sleep(time.Millisecond * 100)
	}
}
*/
func (kv *ShardKV) pullConfig() {
	for {
		newConfig := kv.mck.Query(kv.config.Num + 1)
		if newConfig.Num > kv.config.Num {
			kv.updateConfigShard(newConfig)
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (kv *ShardKV) excuteApply() {
	for apply := range kv.applyCh {
		if apply.UseSnapshot {
			if kv.Debug {
				fmt.Println("UseSnapshot")
			}
			kv.readPersist(apply.Snapshot)
			continue
		}
		op := apply.Command.(Op)

		if kv.Debug || kv.Ldebug { //kv.gid == 102 && kv.me == 0 {
			fmt.Println("-------------------------------=")
			fmt.Println("excute,", op.Type, "shard", "value")
			fmt.Println("index is", apply.Index)
			fmt.Println("ck,seq,", op.Ckid, op.Seq)
			fmt.Println("now config is", kv.config)
			fmt.Println("now ckconfig is", op.CkConfigNum)
			fmt.Println("ckidseq now is", kv.cidSeq)

			fmt.Println("now db is", kv.db)
			fmt.Println("now vaild", kv.isVailedShard)

			fmt.Println("now gid：server", kv.gid, kv.me)
			fmt.Println("-------------------------------=")
		}
		kv.mu.Lock()
		if op.Type == "Get" {
			if kv.isVailedShard[op.Args.(ExcuteGetArgs).Shard] == false || kv.config.Num != op.CkConfigNum {
				op.Err = ErrWrongGroup

			} else {
				exOp := op.Args.(ExcuteGetArgs)
				op.Args = ExcuteGetArgs{Value: kv.db[exOp.Shard][exOp.Key]}
				op.Err = OK
			}
			//	kv.unLockIndex(apply.Index, op)
		} else if maxSeq, success := kv.cidSeq[op.Ckid]; (!success || op.Seq > maxSeq) && (op.Type == "Put" || op.Type == "Append") {
			if kv.isVailedShard[op.Args.(ExcutePutAppendArgs).Shard] == false || kv.config.Num != op.CkConfigNum {
				op.Err = ErrWrongGroup

			} else {
				exOp := op.Args.(ExcutePutAppendArgs)

				switch op.Type {
				case "Put":
					{
						kv.db[exOp.Shard][exOp.Key] = exOp.Value
					}
				case "Append":
					{
						kv.db[exOp.Shard][exOp.Key] += exOp.Value
					}
				}
				op.Err = OK
			}
		} else if op.Type == "InstallShard" || op.Type == "DeleteShard" || op.Type == "NewConfig" || op.Type == "UpdateCidSeq" {
			switch op.Type {
			case "InstallShard":
				{
					exOp := op.Args.(ExcuteInstallShardArgs)
					kv.excuteInstallShard(exOp)
				}
			case "DeleteShard":
				{
					exOp := op.Args.(ExcuteDeleteShardArgs)
					kv.excuteDeleteShard(exOp)
				}
			case "NewConfig":
				{
					exOp := op.Args.(ExcuteNewConfigArgs)
					kv.excuteNewConfig(exOp)
				}
			case "UpdateCidSeq":
				{
					exOp := op.Args.(ExcuteUpdateCidSeqArgs)
					kv.excuteUpdateCidSeq(exOp)
				}
			}
			op.Err = OK

		} else {
			op.Err = "seq"
		}
		kv.mu.Unlock()
		if op.Err == OK {
			kv.cidSeq[op.Ckid] = op.Seq
		}
		kv.unLockIndex(apply.Index, op)
		if kv.maxraftstate != -1 && kv.checkNeedSnapShot() {
			kv.saveNewSnapShot(apply.Index)
		}
	}
}
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(ExcuteInstallShardArgs{})
	gob.Register(ExcuteDeleteShardArgs{})
	gob.Register(ExcuteNewConfigArgs{})
	gob.Register(ExcuteUpdateCidSeqArgs{})
	gob.Register(ExcuteGetArgs{})
	gob.Register(ExcutePutAppendArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.config.Groups = make(map[int][]string)
	//	kv.config = kv.mck.Query(kv.config.Num + 1)
	//	kv.updateConfigShard(kv.config)

	kv.persister = persister
	kv.db = make(map[int]map[string]string)

	kv.chMap = make(map[int]chan Op)
	kv.chWaitMap = make(map[int]chan bool)

	kv.cidSeq = make(map[int64]int64)
	kv.Ckid = nrand()
	kv.dbInstallShard = make(map[int]map[int]map[string]string) //configNum_sid_k_v
	kv.isVailedShard = make(map[int]bool)
	kv.wantShardConfig = make(map[int]int)
	kv.needSendShardConfig = make(map[int]int)
	kv.historyConfig = make(map[int]shardmaster.Config)
	kv.Debug = false
	kv.Ldebug = false
	kv.rf.Debug = false

	kv.readPersist(persister.ReadSnapshot())

	go kv.pullConfig()
	go kv.excuteApply()
	return kv
}
