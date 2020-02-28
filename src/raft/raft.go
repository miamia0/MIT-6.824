package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"labrpc"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// testesr) on the same server, via the applyCh passed to Make().
//

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//

type LogMsg struct {
	Term    int
	Command interface{}
}
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistenet state on all servers

	Debug  bool
	Status int //0 for follwer 1 for candidate 2 for leader

	// persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogMsg //log list( means all recieve and commited )
	// volatile state on all servers
	commitedIndex int
	lastApplied   int

	// volatile state on leaders
	nextIndex  []int // for each server , index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server ,index of highest log entry known to be replicated on server

	//other discuss
	applyCh chan ApplyMsg

	getAppendEntrieschan chan bool
	getRequestVotechan   chan bool
	electionEnd          chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	term = rf.currentTerm
	if rf.Status == LEADER {
		isleader = true
	} else {
		isleader = false
	}
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidatedId int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//RequestVoteReply =v=
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (2A).
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int //index of log entry immediately precceding new ones
	PreLogTerm   int
	Entries      []LogMsg
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 1 {
		return 0
	} else {
		return rf.log[len(rf.log)-1].Term
	}
}
func (rf *Raft) getLastLogIndex() int {

	return len(rf.log) - 1

}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()

	defer rf.mu.Unlock()
	reply.VoteGranted = false
	//	fmt.Println("new vote requests from ", rf.me, args.CandidatedId, rf.Status)
	if args.Term < rf.currentTerm {

	} else if rf.votedFor != args.CandidatedId && rf.currentTerm == args.Term {
		// has selected a leader,at least as uptdate

	} else if args.LastLogTerm < rf.getLastLogTerm() {
		//
	} else if args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < rf.getLastLogIndex() {

	} else {
		if rf.Debug {
			fmt.Printf("Follower %d  grant %d vote\n", rf.me, args.CandidatedId)
		}
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidatedId
		reply.VoteGranted = true
		rf.getRequestVotechan <- true
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) updateAppliedIndex() {
	for rf.lastApplied < rf.commitedIndex {
		rf.lastApplied++

		tmpLog := rf.log[rf.lastApplied]
		applyMsg := ApplyMsg{
			Index:   rf.lastApplied,
			Command: tmpLog.Command,
		}
		rf.applyCh <- applyMsg

	}
}
func (rf *Raft) updateCommitIndex() {
	rf.matchIndex[rf.me] = len(rf.log) - 1
	tmpIndex := make([]int, len(rf.matchIndex))
	copy(tmpIndex, rf.matchIndex)
	sort.Ints(tmpIndex)
	N := tmpIndex[len(tmpIndex)/2]
	if N > rf.commitedIndex && rf.log[N].Term == rf.currentTerm {
		rf.commitedIndex = N
	}
}

//是不是收到heartBeats后发现term比当前要大的话就更新term?并且如果在leader的话就需要变成 follower
// 如果是candidate收到了rpc应该怎么办，
func (rf *Raft) checkLastLog(term int, index int) bool {
	if len(rf.log)-1 < index {
		return false
	} else if rf.log[index].Term != term {
		///if rf.Debug {
		fmt.Printf("rf.log[index(%d)].Term(%d) != term(%d) ", index, rf.log[index].Term, term)
		//}
		return false
	}
	return true

}

//AppendEntries -b-
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.Debug {
		fmt.Printf(" %d  get from %d\n", rf.me, args.LeaderId)
	}
	if args.Term < rf.currentTerm || rf.commitedIndex > args.PrevLogIndex+len(args.Entries) { // it will not appear that  commitedinedx is bigger than PrevIndex + len(entries) for safty reasons which shows that commitedindex is up to date
		reply.Success = false

	} else if rf.checkLastLog(args.PreLogTerm, args.PrevLogIndex) == false {
		fmt.Printf("err!!!!!!!!!!!!!!!!!!!! %d %d\n", args.LeaderId, rf.me)
		reply.Success = false
	} else {
		for i := range args.Entries { // update entries
			newEntrieID := args.PrevLogIndex + i + 1
			newEntrie := args.Entries[i]
			if rf.commitedIndex >= newEntrieID {
				if args.Entries[i].Term != rf.log[newEntrieID].Term {
					rf.log[newEntrieID] = newEntrie
				}
			} else {
				rf.log = append(rf.log, newEntrie)
			}
		}
		if args.LeaderCommit > rf.commitedIndex {
			rf.commitedIndex = Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
			rf.updateAppliedIndex()
		}
		reply.Success = true
		reply.Term = rf.currentTerm
		rf.getAppendEntrieschan <- true

	}
	if rf.Debug {

		fmt.Println(rf.me, "logs: ", rf.log)
	}
	reply.Term = rf.currentTerm

	//fmt.Println(rf.me, " ", rf.log, "<")
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will   at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.Status == LEADER
	if isLeader == true {
		index = rf.getLastLogIndex() + 1

		rf.log = append(rf.log, LogMsg{rf.currentTerm, command})
		if rf.Debug {
			fmt.Println(rf.me, ":", rf.log)
		}
		//rf.lastApplied = rf.lastApplied + 1

	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.Debug = false
}

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

func (rf *Raft) setFollower() {
	if rf.Debug {
		fmt.Println("~~~~~~~~~~~~~~~~~~~~~~~~~~")
		fmt.Println("Follower ", rf.me, " became a Follower! ")
		fmt.Println("~~~~~~~~~~~~~~~~~~~~~~~~~~")

	}
	rf.votedFor = -1
	rf.Status = FOLLOWER
}

func (rf *Raft) setCandidate() {
	rf.Status = CANDIDATE
	if rf.Debug {
		fmt.Println("==========================")
		fmt.Println("Candidate ", rf.me, " became a Candidate! ")
		fmt.Println("==========================")

	}
	rf.votedFor = rf.me
	rf.currentTerm = rf.currentTerm + 1
	go rf.election()

}
func (rf *Raft) setLeader() {
	rf.Status = LEADER
	if rf.Debug {
		fmt.Println("&&&&&&&&&&&&&&&&&&&&&&&&")
		fmt.Println("Leader ", rf.me, " became a leader! ")
		fmt.Println("&&&&&&&&&&&&&&&&&&&&&&&&")

	}
	for i := range rf.peers {
		rf.nextIndex[i] = rf.commitedIndex + 1
		rf.matchIndex[i] = 0
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

//1. vote recieved
//2.curent leader  or new term

func (rf *Raft) election() {
	var wg sync.WaitGroup
	var voteGotLock sync.Mutex
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidatedId: rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	if rf.Debug {
		fmt.Println("Candidate ", rf.me, " start election")
	}
	rf.mu.Lock()
	rf.votedFor = rf.me
	rf.mu.Unlock()
	voteGot := 1 //vote for self
	for serverNumber := range rf.peers {
		if serverNumber != rf.me {
			wg.Add(1)
			go func(serverNumber int) {
				var reply RequestVoteReply
				defer wg.Add(-1)
				work := rf.sendRequestVote(serverNumber, &args, &reply)
				if rf.Status != CANDIDATE {
					return
				}

				if work {
					if reply.VoteGranted {
						voteGotLock.Lock()
						voteGot++
						voteGotLock.Unlock()
						if voteGot*2 > len(rf.peers) {
							rf.setLeader()
							if rf.Debug {
								fmt.Printf("Follower %d become Leader with %d ,serverNumber is %d\n", rf.me, voteGot, serverNumber)
							}
							rf.electionEnd <- true
						}
					} else if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.setFollower()
						rf.electionEnd <- true
					}
				}
			}(serverNumber)
		}
	}
	wg.Wait()
}

func (rf *Raft) getPrelogTerm(serverNumber int) int {
	if len(rf.log) == 1 {
		return 0
	} else {
		return rf.log[rf.nextIndex[serverNumber]-1].Term
	}

}

//heartBeats send heartBeats
func (rf *Raft) heartBeats() {
	var wg sync.WaitGroup
	for serverNumber := range rf.peers {
		//fmt.Printf("%d send  ents %d \n",rf.me,rf.currentTerm)
		if serverNumber != rf.me {
			wg.Add(1)
			go func(serverNumber int) {
				var reply AppendEntriesReply
				defer wg.Add(-1)
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[serverNumber] - 1,
					PreLogTerm:   rf.getPrelogTerm(serverNumber), // rf.log[rf.nextIndex[serverNumber]].Term, //可是觉得和这个没有什么关系啊···==？？？？？
					Entries:      append([]LogMsg{}, rf.log[Min(len(rf.log), rf.nextIndex[serverNumber]):]...),
					LeaderCommit: rf.commitedIndex,
				}

				work := rf.sendAppendEntries(serverNumber, &args, &reply)
				if rf.Status != LEADER {
					return
				}
				if work == false {
				} else {
					if reply.Success {
						rf.mu.Lock()

						rf.matchIndex[serverNumber] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[serverNumber] = rf.matchIndex[serverNumber] + 1
						rf.updateCommitIndex()
						rf.updateAppliedIndex()

						rf.mu.Unlock()

					} else {
						rf.mu.Lock()
						rf.nextIndex[serverNumber]--
						if rf.nextIndex[serverNumber] <= 0 {
							fmt.Println("error in Leader", rf.me, "append =>", serverNumber, " with rf.nextIndex[serverNumber] ", rf.nextIndex[serverNumber])
						}
						rf.mu.Unlock()
					}

					if reply.Term > rf.currentTerm { //higher term
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.setFollower()
						rf.mu.Unlock()
					}
				}
			}(serverNumber)
		}
	}
	wg.Wait()
}

func (rf *Raft) startServer() { //处理状态问题,
	heartbeatTimer := time.Duration(300) * time.Millisecond

	go func() { //监听 append entries 和 request vote 管理timer
		for {
			followerTimer := time.Duration((300 + rand.Float64()*1000) * (float64)(time.Millisecond))
			candidateTimer := time.Duration((300 + rand.Float64()*1000) * (float64)(time.Millisecond))
			switch rf.Status {
			case FOLLOWER:
				{
					select {
					case <-rf.getAppendEntrieschan:
					case <-rf.getRequestVotechan:
					case <-time.After(followerTimer):
						rf.setCandidate()
					}
				}
			case CANDIDATE:
				{
					select {
					case <-rf.electionEnd:
					case <-rf.getAppendEntrieschan:
					case <-time.After(candidateTimer):
						rf.setCandidate()
					}
				}
			case LEADER:
				{
					rf.heartBeats()
					time.Sleep(heartbeatTimer)
				}
			}
		}
	}()

}

//log的id 从0开始
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.Debug = true
	if rf.Debug {
		fmt.Printf("init server -----%d \n", rf.me)
	}
	rf.Status = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitedIndex = 0
	rf.lastApplied = 0
	peerCnt := len(peers)
	for i := 0; i < peerCnt; i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	//start from 1 0 is scared
	rf.log = append(rf.log, LogMsg{Term: 0})
	rf.electionEnd = make(chan bool)
	rf.getAppendEntrieschan = make(chan bool)
	rf.getRequestVotechan = make(chan bool)
	go rf.startServer()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
