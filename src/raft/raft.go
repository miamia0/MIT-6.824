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
// tester) on the same server, via the applyCh passed to Make().
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
	getRPCchan chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = (rf.Status == LEADER)
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
	term         int
	candidatedId int
	lastLogIndex int
	lastLogTerm  int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	term        int
	voteGranted bool
	// Your data here (2A).
}

type AppendEntriesArgs struct {
	term         int
	leaderId     int
	prevLogIndex int
	preLogTerm   int
	entries      []byte
	leaderCommit int
}
type AppendEntriesReply struct {
	term    int
	success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//	reply = &RequestVoteReply{}

	rf.mu.Lock()
	fmt.Printf("server %d  get requestsVote from %d\n", rf.me, args.candidatedId)

	defer rf.mu.Unlock()
	fmt.Printf("server %d  get requestsVote from %d\n", rf.me, args.candidatedId)

	rf.getRPCchan <- true

	if args.term < rf.currentTerm {
		reply.term = rf.currentTerm
		reply.voteGranted = false

	} else if rf.votedFor != -1 && rf.votedFor != args.candidatedId || args.lastLogIndex >= rf.lastApplied { // at least as uptodate
		reply.term = rf.currentTerm
		reply.voteGranted = false

	} else {
		reply.term = rf.currentTerm
		reply.voteGranted = true
		rf.votedFor = args.candidatedId
	}

}

//是不是收到heartbeats后发现term比当前要大的话就更新term?并且如果在leader的话就需要变成 follower
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.getRPCchan <- true
	reply = &AppendEntriesReply{}
	if args.term < rf.currentTerm {
		reply.success = false
		reply.term = args.term
	} else {
		reply.success = true
		reply.term = args.term
		rf.currentTerm = args.term
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
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
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

	tickerTime := time.Duration((0.5 + rand.Float64()*0.5) * (float64)(time.Second))
	ticker := time.NewTicker(tickerTime)
	nxtStatus := -1
	var wg sync.WaitGroup
	wg.Add(1)
	go func(ticker *time.Ticker) {
		defer func() {
			ticker.Stop()
			wg.Add(-1)

		}()
		for {
			select {
			case <-ticker.C:
				return

			case stop := <-rf.getRPCchan:
				if stop {
					nxtStatus = FOLLOWER
					return
				}
			}
		}
	}(ticker)
	wg.Wait()

	if nxtStatus == -1 {
		rf.setCandidate()
		nxtStatus = CANDIDATE
	}
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
	//timer
	nxtStatus := CANDIDATE
	var wg sync.WaitGroup
	var timerwg sync.WaitGroup

	CandidateChan := make(chan int) // when get a
	tickerTime := time.Duration((0.5 + rand.Float64()*0.5) * (float64)(time.Second))
	ticker := time.NewTicker(tickerTime)
	timerwg.Add(1)

	go func(ticker *time.Ticker) {
		defer func() {
			ticker.Stop()
			timerwg.Add(-1)
		}()

		for {
			select {
			case <-ticker.C:
				return

			case nxtStatus = <-CandidateChan:
				{
					rf.Status = nxtStatus
					return
				}
			}
		}
	}(ticker)

	//RequestVote
	args := RequestVoteArgs{}

	rf.currentTerm = rf.currentTerm + 1
	args.term = rf.currentTerm
	args.candidatedId = rf.me
	args.lastLogIndex = rf.lastApplied //
	args.lastLogTerm = rf.currentTerm  //

	voteGot := 0
	voteGot++

	for server_number, _ := range rf.peers {
		if server_number != rf.me {
			wg.Add(1)

			go func() {
				var reply RequestVoteReply

				work := rf.sendRequestVote(server_number, args, &reply) //给所有server发送请求
				fmt.Printf("server %d send questvote %d\n", rf.me, server_number)

				if work != false {
					fmt.Printf("err in send requestsvote to %d\n", server_number)
				}

				if reply.voteGranted {

					voteGot++
				} else if reply.term > rf.currentTerm {
					CandidateChan <- FOLLOWER
				}
				defer wg.Add(-1)
			}()
		} else {
			rf.votedFor = rf.me
		}

	}
	//statue change

	wg.Wait()
	fmt.Printf("server %d voteGot %d \n", rf.me, voteGot)

	if voteGot*2 > len(rf.peers) {
		CandidateChan <- LEADER
	} else {
		CandidateChan <- CANDIDATE
	}
	timerwg.Wait()
	return nxtStatus

}
func (rf *Raft) Leader() int {
	//send heartbeats
	var wg sync.WaitGroup
	nxtStatus := LEADER
	args := AppendEntriesArgs{}
	higherTermchan := make(chan bool)
	for server_number, _ := range rf.peers {
		if server_number != rf.me {
			wg.Add(1)
			go func(rf *Raft) {
				var reply AppendEntriesReply
				work := rf.sendAppendEntries(server_number, args, &reply) //给所有server发送请求
				fmt.Println("sented")
				if work != true {
					fmt.Printf("err in send requestsvote to %d\n", server_number)
				}
				//deal with reply
				if reply.success == true && reply.term > rf.currentTerm { //higher term
					higherTermchan <- true
				}
				defer wg.Add(-1)
			}(rf)
		}
	}
	wg.Add(1)
	go func() {
		<-higherTermchan
		rf.Follower()
		nxtStatus = FOLLOWER
		defer wg.Add(-1)
	}()
	wg.Wait()
	return nxtStatus

	//discover higher term

}
func (rf *Raft) startServer() { //处理状态问题,

	go func(rf *Raft) { //candidate status

		//	higherTermchan := make(chan bool)
		for { //切换状态后才算一次执行结束。
			fmt.Printf("server started %d status %d\n", rf.me, rf.Status)

			switch rf.Status {
			case FOLLOWER:
				rf.Follower()
			case CANDIDATE:
				rf.Candidate()
			case LEADER:
				rf.Leader()
			}
		}

	}(rf)

}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.Status = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitedIndex = 0
	rf.lastApplied = 0
	//rf.nextIndex = 0
	//rf.matchIndex = 0
	rf.getRPCchan = make(chan bool)
	go rf.startServer()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
