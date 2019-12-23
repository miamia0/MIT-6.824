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
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistenet state on all servers

	Status int //0 for follwer 1 for candidate 2 for leader

	currentTerm int
	votedFor    int
	//log []

	commitedIndex int
	lastApplied   int

	nextIndex  []int
	matchIndex []int
	getAppendEntrieschan chan bool
	getRequestVotechan chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	term = rf.currentTerm
	if rf.Status == LEADER{
		isleader =true
	}else{
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
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (2A).
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PreLogTerm   int
	Entries      []byte
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()

	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {

		reply.Term = rf.currentTerm
		reply.VoteGranted = false

	} else if rf.votedFor != args.CandidatedId && rf.currentTerm == args.Term  { // at least as uptodate
		reply.Term = rf.currentTerm 
		reply.VoteGranted = false
	
	}else {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidatedId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.getRequestVotechan <- true

	}
}

//是不是收到heartbeats后发现term比当前要大的话就更新term?并且如果在leader的话就需要变成 follower
// 如果是candidate收到了rpc应该怎么办，

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		reply.Success = true
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.getAppendEntrieschan <- true
	}
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
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
}

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

func (rf *Raft) setFollower() {
	rf.Status = FOLLOWER
}

func (rf *Raft) setCandidate() {
	rf.Status = CANDIDATE

}
func (rf *Raft) setLeader() {
	rf.Status = LEADER
}
func (rf *Raft) Follower() int {

	tickerTime := time.Duration((1 + rand.Float64()*1) * (float64)(time.Second))
	ticker := time.NewTicker(tickerTime)
	nxtStatus := CANDIDATE
	var wg sync.WaitGroup
	wg.Add(1)

	go func(ticker *time.Ticker) {                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
		defer func() {
			ticker.Stop()
			wg.Add(-1)

		}()
		for {
			select {
			case <-ticker.C:{
				return
			}

			case stop := <-rf.getAppendEntrieschan:
				if stop {
					nxtStatus = FOLLOWER
					return
				}
			case stop := <-rf.getRequestVotechan:
				if stop {

					nxtStatus = FOLLOWER
					return
				}
			}
		}
	}(ticker)
	wg.Wait()
	rf.Status = nxtStatus

	//得到了vote
	return nxtStatus
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

func (rf *Raft) Candidate() int {
	nxtStatus := CANDIDATE
	var wg sync.WaitGroup
	var timerwg sync.WaitGroup

	CandidateResultChan := make(chan int) // when get a
	tickerTime := time.Duration((1 + rand.Float64()*1)* (float64)(time.Second))
	ticker := time.NewTicker(tickerTime)
	

	rf.currentTerm = rf.currentTerm + 1
	fmt.Printf("%d become a candidate in Term %d\n",rf.me,rf.currentTerm)

	args := RequestVoteArgs{
		Term:rf.currentTerm,
		CandidatedId:rf.me,
		LastLogIndex:rf.lastApplied,
		LastLogTerm:rf.currentTerm,
	}
	voteGot := 0
	voteGot++

	var voteGotLock sync.Mutex
	timerwg.Add(1)
	electionEnded := false

	go func(ticker *time.Ticker) {
		defer func() {
			ticker.Stop()
			timerwg.Add(-1)
		}()

		for {
			select {
			case <-ticker.C:
				{
					rf.Status = CANDIDATE
					electionEnded = true //超时中止
					return
				}
			case  <- rf.getAppendEntrieschan: //遇见新的term的leader
				{
					electionEnded = true 
					rf.Status = FOLLOWER
					return
				}
			case nxtStatus = <-CandidateResultChan:
				{
					rf.Status = nxtStatus
					return
				}
			}
		}
	}(ticker)

	//RequestVote

	
	for server_number, _ := range rf.peers {
		if server_number != rf.me {
			wg.Add(1)
			go func(server_number int) {
				var reply RequestVoteReply
				defer wg.Add(-1)
				fmt.Printf("server %d send %d vote requests in term %d\n",rf.me,server_number,rf.currentTerm)
				work := rf.sendRequestVote(server_number, &args, &reply) //给所有server发送请求,因为觉得这个来回应该不会比较慢所以不管了
				if electionEnded {
					return
				}
				if work == false {
					fmt.Printf("network err %d in send requestsvote to %d\n",rf.me ,server_number)
				}else if reply.VoteGranted {
					voteGotLock.Lock()
					voteGot++
					if voteGot*2 > len(rf.peers) && electionEnded == false{
						fmt.Printf("%d become a leader\n",rf.me)
						CandidateResultChan <- LEADER//Win
						electionEnded = true
					}
					voteGotLock.Unlock()

				} else if reply.Term > rf.currentTerm {
					CandidateResultChan <- FOLLOWER
					electionEnded = true //lose
					rf.currentTerm = reply.Term
				}
			}(server_number)
		} else { 
			rf.votedFor = rf.me
		}

	}

	timerwg.Wait()
	wg.Wait()
	return nxtStatus
}
func (rf *Raft) Leader() int {
	var wg sync.WaitGroup
	var timerwg sync.WaitGroup
	nxtStatus := LEADER

	tickerTime := time.Duration(0.4* (float64)(time.Second))
	ticker := time.NewTicker(tickerTime)
	timerwg.Add(1)
	higherTermchan := make(chan bool)
	timerEnded := false
	go func(ticker *time.Ticker) {
		defer func() {
			ticker.Stop()
			timerwg.Add(-1)
		}()

		for {
			select {
			case <-ticker.C:{
				timerEnded = true
				return
			}

			case <-higherTermchan:
				{

					rf.Status = FOLLOWER
					return
				}
			case <-rf.getAppendEntrieschan:
				{
					rf.Status = FOLLOWER
					return
				}
			case  <-rf.getRequestVotechan:
				{

					rf.Status = FOLLOWER
					return
				}
		
			}
		}
	}(ticker)

	//send heartbeats
	args := AppendEntriesArgs{
		Term         :rf.currentTerm,
		LeaderId     :rf.me,
		PrevLogIndex :0,
		PreLogTerm   :0,
		LeaderCommit :rf.commitedIndex,
	}
	for server_number, _ := range rf.peers {
		//fmt.Printf("%d send  ents %d \n",rf.me,rf.currentTerm)

		if server_number != rf.me {
			wg.Add(1)
			go func(server_number int) {
				var reply AppendEntriesReply
				defer wg.Add(-1)

				work := rf.sendAppendEntries(server_number, &args, &reply) //给所有server发送请求
				if work == false {
					fmt.Printf("err in send appendentries to %d\n", server_number)
				}

				if work&& reply.Term > rf.currentTerm { //higher term
					rf.currentTerm = reply.Term
					fmt.Printf("?!server : %d next: %d term:%d higher term:%d \n", rf.me,server_number,rf.currentTerm,reply.Term)
					if rf.Status == LEADER && timerEnded == false{
						higherTermchan <- true
					}
				}
			}(server_number)
		}
	}
	timerwg.Wait()
	wg.Wait()
	return nxtStatus

	//discover higher term

}
func (rf *Raft) startServer() { //处理状态问题,

	go func() { //candidate status

		//	higherTermchan := make(chan bool)
		for { //切换状态后才算一次执行结束。
			//			fmt.Printf("server started %d status %d\n", rf.me, rf.Status)
			//切换状态
		switch rf.Status {
			case FOLLOWER:
				rf.Follower()
			case CANDIDATE:
				rf.Candidate()
			case LEADER:
				rf.Leader()
			}
		}

	}()

}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	fmt.Printf("init server -----%d \n",rf.me)
	rf.Status = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitedIndex = 0
	rf.lastApplied = 0
	//rf.nextIndex = 0
	//rf.matchIndex = 0
	rf.getAppendEntrieschan = make(chan bool)
	rf.getRequestVotechan = make(chan bool)
	go rf.startServer()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
