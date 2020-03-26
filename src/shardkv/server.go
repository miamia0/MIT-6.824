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
	Type      string
	Key       string
	Err       Err
	Value     string
	Ckid      int64
	Seq       int64
	Shard     int
	ConfigNum int
	Config    shardmaster.Config

	Data     map[string]string
	CKid2Seq map[int64]int64
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

	Ckid          int64
	seq           int64
	mck           *shardmaster.Clerk
	config        shardmaster.Config
	mediateConfig shardmaster.Config
	persister     *raft.Persister
	// Your definitions here.
	needSendShardConfig   map[int]int
	installShardconfigNum map[int]map[int]bool              //sid_configNum
	dbInstallShard        map[int]map[int]map[string]string //sid_k_v
	historyConfig         map[int]shardmaster.Config
	isVailedShard         map[int]bool
	wantShardConfig       map[int]int
	configNumDeleteShard  map[int][]int //configNum_sidlist
	configNumMatchCount   map[int]int
	groupUpdatedConfig    map[int]int

	Debug bool
}

func (kv *ShardKV) checkNeedSnapShot() bool {
	if kv.maxraftstate < kv.persister.RaftStateSize() {
		return true
	}
	return false
}

func (kv *ShardKV) saveNewSnapShot(index int) bool {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	kv.mu.Lock()
	e.Encode(kv.Ckid)
	e.Encode(kv.seq)
	e.Encode(kv.db)
	e.Encode(kv.cidSeq)
	e.Encode(kv.config)
	e.Encode(kv.configNumDeleteShard)
	e.Encode(kv.dbInstallShard)
	e.Encode(kv.installShardconfigNum)
	e.Encode(kv.isVailedShard)
	e.Encode(kv.wantShardConfig)
	e.Encode(kv.needSendShardConfig)

	kv.mu.Unlock()
	if kv.Debug {
		fmt.Println(kv.me, " server saveNewSnapShot for index ", index)
	}
	data := w.Bytes()
	kv.rf.SaveNewSnapShotRaft(index, data)

	return false
}
func (kv *ShardKV) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	kv.mu.Lock()
	d.Decode(&kv.Ckid)
	d.Decode(&kv.seq)
	d.Decode(&kv.db)
	d.Decode(&kv.cidSeq)
	d.Decode(&kv.config)
	d.Decode(&kv.configNumDeleteShard)
	d.Decode(&kv.dbInstallShard)
	d.Decode(&kv.installShardconfigNum)

	d.Decode(&kv.isVailedShard)
	d.Decode(&kv.wantShardConfig)

	d.Decode(&kv.needSendShardConfig)
	kv.mu.Unlock()

}

func (kv *ShardKV) getLock(index int) chan Op {
	//	fmt.Println("getLock lock mu")
	if _, success := kv.chMap[index]; success == false {
		kv.chMap[index] = make(chan Op, 1)
	}
	//	fmt.Println("getLock release mu")

	return kv.chMap[index]
}
func (kv *ShardKV) getWaitLock(index int) chan bool {
	//	fmt.Println("getLock lock mu")
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
	case <-time.After(time.Second):
		<-kv.getWaitLock(logIndex)
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
	// Your code here.
	originOp := Op{
		Type:  "Get",
		Key:   args.Key,
		Ckid:  args.Ckid,
		Seq:   args.Seq,
		Shard: args.Shard,
	}
	//sid := args.Shard
	if kv.Debug {
		fmt.Println(kv.me, "excute Get ", args)
	}

	logIndex, _, isLeader := kv.rf.Start(originOp)
	if !isLeader {
		return
	}
	op := kv.lockIndex(logIndex)

	if equalOp(originOp, op) {
		if kv.Debug {

			fmt.Println("gid,", kv.gid, kv.me, "ended a Get RPC", args, "value", op.Value, "err", op.Err)
		}
		reply.WrongLeader = false
		reply.Value = op.Value
		reply.Err = op.Err
		return
	}
}

//PutAppend RPC
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.WrongLeader = true
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}
	originOp := Op{Type: args.Op,
		Key:   args.Key,
		Value: args.Value,
		Ckid:  args.Ckid,
		Seq:   args.Seq,
		Shard: args.Shard,
	}
	if kv.Debug {
		fmt.Println("gid,", kv.gid, kv.me, "recive  a PutAppend RPC", args)
	}

	logIndex, _, isLeader := kv.rf.Start(originOp)
	if !isLeader {
		return
	}
	op := kv.lockIndex(logIndex)

	if equalOp(originOp, op) {
		if kv.Debug {
			fmt.Println("gid,", kv.gid, kv.me, "ended a PutAppend RPC", args, "op.Err", op, "originOp", originOp)
		}
		reply.WrongLeader = false
		reply.Err = op.Err
		return
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
type InstallShardArgs struct {
	Data     map[string]string
	CKid2Seq map[int64]int64

	Shard     int
	Config    shardmaster.Config
	ConfigNum int
	Ckid      int64
	Seq       int64
}
type InstallShardReply struct {
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
		Type:      "InstallShard",
		Shard:     args.Shard,
		Data:      args.Data,
		Ckid:      args.Ckid,
		Seq:       args.Seq,
		ConfigNum: args.ConfigNum,
		CKid2Seq:  args.CKid2Seq,
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
		Type:      "DeleteShard",
		Shard:     args.Shard,
		Ckid:      kv.Ckid,
		Seq:       atomic.AddInt64(&kv.seq, 1),
		ConfigNum: args.ConfigNum,
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
		fmt.Println("installed num is,", kv.installShardconfigNum)
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
		Type:   "NewConfig",
		Ckid:   kv.Ckid,
		Seq:    atomic.AddInt64(&kv.seq, 1),
		Config: Config,
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
func (kv *ShardKV) excuteNewConfig(op Op) {

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
			} else if _, success := kv.dbInstallShard[newConfig.Num][Sid]; success {
				kv.db[Sid] = kv.dbInstallShard[newConfig.Num][Sid]
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
					Data:      kv.db[Sid],
					CKid2Seq:  kv.cidSeq,
					Shard:     Sid,
					Config:    newConfig,
					ConfigNum: newConfig.Num,
					Ckid:      kv.Ckid,
					Seq:       atomic.AddInt64(&kv.seq, 1),
				}

				go kv.sendShardToGroup(servers, args)
			} else {
				kv.needSendShardConfig[Sid] = newConfig.Num
			}
			/*
				argsDelete := &DeleteShardArgs{
					Shard:     Sid,
					ConfigNum: newConfig.Num,
				}
				reply := DeleteShardReply{}
				go kv.DeleteShard(argsDelete, &reply)
			*/
			kv.isVailedShard[Sid] = false
		} else if Gid == kv.gid && kv.config.Shards[Sid] == kv.gid {
			//not changed
		} else {
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
func (kv *ShardKV) excuteDeleteShard(op Op) {
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
	kv.configNumDeleteShard[configNum] = append(kv.configNumDeleteShard[configNum], sid)
}
func Max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
func (kv *ShardKV) excuteInstallShard(op Op) {

	sid := op.Shard
	configNum := op.ConfigNum
	ckid2eq := op.CKid2Seq
	data := op.Data
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
			Data:      data,
			CKid2Seq:  kv.cidSeq,
			Shard:     sid,
			Config:    needSendConfig,
			ConfigNum: needSendConfig.Num,
			Ckid:      kv.Ckid,
			Seq:       atomic.AddInt64(&kv.seq, 1),
		}
		kv.needSendShardConfig[configNum] = 0
		go kv.sendShardToGroup(servers, args)
	}
	for ckid, seq := range ckid2eq {
		kv.cidSeq[ckid] = Max(kv.cidSeq[ckid], seq)
	}
	if configNum == kv.wantShardConfig[sid] && kv.isVailedShard[sid] == false {
		if kv.config.Shards[sid] == kv.gid {
			kv.db[sid] = data
			kv.isVailedShard[sid] = true
		} else {

		}
	} else if configNum > kv.wantShardConfig[sid] {
		if _, success := kv.dbInstallShard[configNum]; !success {
			kv.dbInstallShard[configNum] = make(map[int]map[string]string)
		}
		kv.dbInstallShard[configNum][sid] = data
	}
	if kv.Debug {
		fmt.Println("==========================")
		fmt.Println(kv.gid, kv.me, "excuteInstallShard", "sid:", sid, "configNum:", configNum)
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
		if kv.Debug {
			fmt.Println("-------------------------------=")
			fmt.Println("excute,", op.Type, "shard", op.Shard, "key", op.Key, "value", op.Value, "configNum", op.ConfigNum)
			fmt.Println("ck,seq,", op.Ckid, op.Seq)
			fmt.Println("now config is", kv.config)
			fmt.Println("now db is", kv.db)
			fmt.Println("now vaild", kv.isVailedShard)

			fmt.Println("now gid：server", kv.gid, kv.me)
			fmt.Println("-------------------------------=")
		}
		kv.mu.Lock()
		if kv.Debug {
			fmt.Println(op.Seq, "get mu lock")
		}
		if kv.isVailedShard[op.Shard] == false && (op.Type == "Put" || op.Type == "Append" || op.Type == "Get") {
			op.Err = ErrWrongGroup
			//	kv.unLockIndex(apply.Index, op)
		} else if op.Type == "Get" {
			op.Value = kv.db[op.Shard][op.Key]
			op.Err = OK
			//	kv.unLockIndex(apply.Index, op)
		} else if maxSeq, success := kv.cidSeq[op.Ckid]; (!success || op.Seq > maxSeq) && (op.Type == "Put" || op.Type == "Append") {
			switch op.Type {
			case "Put":
				{
					kv.db[op.Shard][op.Key] = op.Value
				}
			case "Append":
				{
					kv.db[op.Shard][op.Key] += op.Value
				}
			}
			op.Err = OK

			//	if kv.unLockIndex(apply.Index, op) == true {
			kv.cidSeq[op.Ckid] = op.Seq
			//}
		} else if op.Type == "InstallShard" || op.Type == "DeleteShard" || op.Type == "NewConfig" {
			switch op.Type {
			case "InstallShard":
				{
					kv.excuteInstallShard(op)
				}
			case "DeleteShard":
				{
					kv.excuteDeleteShard(op)
				}
			case "NewConfig":
				{
					kv.excuteNewConfig(op)
				}
			}
			op.Err = OK
			//	kv.unLockIndex(apply.Index, op)
		} else {
			if kv.Debug {
				fmt.Println("err in seq", kv.cidSeq[op.Ckid])
			}
			op.Err = "seq"
			//	kv.unLockIndex(apply.Index, op)
		}
		kv.mu.Unlock()
		kv.unLockIndex(apply.Index, op)
		if kv.maxraftstate != -1 && kv.checkNeedSnapShot() {
			kv.saveNewSnapShot(apply.Index)
		}
		if kv.Debug {

			fmt.Println(op.Seq, "release mu lock")
		}
	}
}
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

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

	kv.persister = persister
	kv.db = make(map[int]map[string]string)

	kv.chMap = make(map[int]chan Op)
	kv.chWaitMap = make(map[int]chan bool)

	kv.cidSeq = make(map[int64]int64)
	kv.Ckid = nrand()
	kv.installShardconfigNum = make(map[int]map[int]bool)       //sid_configNum
	kv.dbInstallShard = make(map[int]map[int]map[string]string) //configNum_sid_k_v
	kv.configNumDeleteShard = make(map[int][]int)               //configNum_sidlist
	kv.isVailedShard = make(map[int]bool)
	kv.wantShardConfig = make(map[int]int)
	kv.needSendShardConfig = make(map[int]int)
	kv.historyConfig = make(map[int]shardmaster.Config)
	kv.Debug = false
	kv.rf.Debug = false

	kv.readPersist(persister.ReadSnapshot())

	go kv.pullConfig()
	go kv.excuteApply()
	return kv
}
