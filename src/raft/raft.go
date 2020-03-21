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
	"bytes"
	"encoding/gob"
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
	debugLog    []LogMsg
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
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	rf.mu.Lock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	rf.mu.Unlock()
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
	PrevLogTerm  int
	Entries      []LogMsg
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term       int
	LastCommit int
	Success    bool
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
	reply.Term = rf.currentTerm

	if rf.Status == LEADER {
		return
	}
	//	fmt.Println("new vote requests from ", rf.me, args.CandidatedId, rf.Status)
	if args.Term < rf.currentTerm {
		if rf.Debug {
			fmt.Printf("Follower %d  !!not grant %d vote in  term %d for term\n", rf.me, args.CandidatedId, args.Term)
		}
	} else if rf.votedFor != -1 && rf.currentTerm == args.Term {
		// has selected a leader,at least as uptdate
		if rf.Debug {
			fmt.Printf("Follower %d  !!not grant %d vote in  term %d for votefor\n", rf.me, args.CandidatedId, args.Term)
		}
	} else if args.LastLogTerm < rf.getLastLogTerm() { //for follower
		rf.currentTerm = args.Term

		if rf.Debug {
			fmt.Printf("Follower %d  !!not grant %d vote in  term %d for LastLogTerm %d <%d\n", rf.me, args.CandidatedId, args.Term, args.LastLogTerm, rf.getLastLogTerm())
		}
	} else if args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < rf.getLastLogIndex() { //for follower

		rf.currentTerm = args.Term
		if rf.Debug {
			fmt.Printf("Follower %d  !!not grant %d vote in  term %d for getLastLogIndex\n", rf.me, args.CandidatedId, args.Term)
		}
	} else {

		if rf.Debug {
			fmt.Printf("Follower %d  grant %d vote in  term %d\n", rf.me, args.CandidatedId, args.Term)
		}
		rf.getRequestVotechan <- true
		rf.setFollower(args.Term)
		rf.votedFor = args.CandidatedId
		reply.VoteGranted = true
		rf.persist()
		reply.Term = rf.currentTerm
	}

}

func (rf *Raft) updateAppliedIndex() {
	for rf.lastApplied < rf.commitedIndex {
		rf.lastApplied++

		tmpLog := rf.log[rf.lastApplied]
		applyMsg := ApplyMsg{
			Index:   rf.lastApplied,
			Command: tmpLog.Command,
		}
		if rf.Debug {
			fmt.Println(rf.me, " commitedIndex to be a", rf.commitedIndex)
		}
		rf.applyCh <- applyMsg

	}
}
func (rf *Raft) updateCommitIndex() {
	rf.matchIndex[rf.me] = len(rf.log) - 1
	tmpIndex := make([]int, len(rf.matchIndex))
	copy(tmpIndex, rf.matchIndex)
	sort.Ints(tmpIndex)
	N := tmpIndex[(len(tmpIndex)-1)/2]
	if N > rf.commitedIndex && rf.log[N].Term == rf.currentTerm {
		rf.commitedIndex = N
	}
}

func (rf *Raft) checkPrevLog(term int, index int) bool {
	if len(rf.log)-1 < index {
		return false
	} else if rf.log[index].Term != term {
		if rf.Debug {
			fmt.Printf("rf.log[index(%d)].Term(%d) != term(%d)\n ", index, rf.log[index].Term, term)
		}
		return false
	}
	return true

}

//AppendEntries -b-
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.LastCommit = rf.commitedIndex
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm { //|| rf.commitedIndex >= args.PrevLogIndex+len(args.Entries) {
		reply.Success = false
		reply.LastCommit = -1

		// whitch means "I will be the next leader you idiots"

	} else if rf.checkPrevLog(args.PrevLogTerm, args.PrevLogIndex) == false {
		//i want earlier log, i am a follower and wait for you
		rf.setFollower(args.Term)
		rf.votedFor = args.LeaderId
		rf.persist()
		reply.LastCommit = rf.commitedIndex
		rf.getAppendEntrieschan <- true
		if rf.Debug {
			fmt.Printf("err in  %d %d args.PrevLogTerm:%d args.PrevLogIndex:%d term is %d\n", args.LeaderId, rf.me, args.PrevLogTerm, args.PrevLogIndex, args.Term)
		}
		reply.Success = false

	} else {
		rf.setFollower(args.Term)
		rf.votedFor = args.LeaderId
		for i := range args.Entries { // update entries
			newEntrieID := args.PrevLogIndex + i + 1
			if len(rf.log) > newEntrieID {
				if args.Entries[i].Term != rf.log[newEntrieID].Term {
					rf.log = rf.log[:newEntrieID]
					rf.log = append(rf.log, args.Entries[i:]...)
					rf.persist()
					rf.debugLog = make([]LogMsg, len(rf.log))
					for _, logs := range rf.log {
						value, ok := logs.Command.(int)
						if ok {
							rf.debugLog[i] = LogMsg{logs.Term, value % 1000}
						}
					}
					break
				}
			} else {
				rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
				rf.debugLog = make([]LogMsg, len(rf.log))
				for _, logs := range rf.log {
					value, ok := logs.Command.(int)
					if ok {
						rf.debugLog[i] = LogMsg{logs.Term, value % 1000}
					}
				}
				break
			}
		}
		if args.LeaderCommit > rf.commitedIndex {
			rf.commitedIndex = Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
			rf.updateAppliedIndex()
		}
		reply.Success = true
		reply.Term = rf.currentTerm
		rf.getAppendEntrieschan <- true

		if len(args.Entries) != 0 && rf.Debug {

			fmt.Println(rf.me, "logs: ", rf.debugLog, "get from ", args.LeaderId, " term is ", args.Term, "commited is ", rf.commitedIndex)
		}
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
		rf.persist()

		value, ok := command.(int)

		if ok {
			rf.debugLog = append(rf.debugLog, LogMsg{rf.currentTerm, value % 1000})
		}

		if rf.Debug {
			fmt.Println("Leader ", rf.me, "get new log :", rf.debugLog)
		}

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

func (rf *Raft) setFollower(newTerm int) {

	rf.Status = FOLLOWER
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.persist()
	if rf.Debug {
		//	fmt.Println("~~~~~~~~~~~~~~~~~~~~~~~~~~")
		//	fmt.Println("Follower ", rf.me, " became a Follower! ")
		//	fmt.Println("~~~~~~~~~~~~~~~~~~~~~~~~~~")

	}
}

func (rf *Raft) setCandidate() {
	rf.Status = CANDIDATE
	rf.votedFor = rf.me
	rf.currentTerm = rf.currentTerm + 1
	rf.persist()

	if rf.Debug {
		fmt.Println("==========================")
		fmt.Println("Candidate ", rf.me, " became a Candidate! in term :", rf.currentTerm)

		fmt.Println("with log ", rf.debugLog)

		fmt.Println("==========================")

	}
	go rf.election()

}
func (rf *Raft) setLeader() {
	rf.Status = LEADER
	if rf.Debug {
		fmt.Println("&&&&&&&&&&&&&&&&&&&&&&&&")
		fmt.Println("Leader ", rf.me, " became a leader! in term ", rf.currentTerm)
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
	var voteGotLock sync.Mutex
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidatedId: rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	if rf.Debug {
		fmt.Println("Candidate ", rf.me, " start election")
		if rf.Debug {
			//	fmt.Println("candidate :", rf.me, "with log :", rf.log)
		}
	}
	rf.mu.Lock()
	rf.votedFor = rf.me
	rf.mu.Unlock()
	rf.persist()
	voteGot := 1 //vote for self
	for serverNumber := range rf.peers {
		if serverNumber != rf.me {
			go func(serverNumber int) {
				var reply RequestVoteReply
				if rf.Status != CANDIDATE {
					return
				}
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
						rf.setFollower(reply.Term)
						rf.electionEnd <- true
					}
				}
			}(serverNumber)
		}
	}
}

func (rf *Raft) getPrelogTerm(serverNumber int) int {
	if len(rf.log) == 1 {
		return 0
	} else {
		if rf.nextIndex[serverNumber] == 0 {
			return 0
		}
		return rf.log[rf.nextIndex[serverNumber]-1].Term
	}

}
func (rf *Raft) getAppendEntriesArgsViaServer(serverNumber int) *AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[serverNumber] - 1,
		PrevLogTerm:  rf.getPrelogTerm(serverNumber),
		Entries:      append([]LogMsg{}, rf.log[Min(len(rf.log), rf.nextIndex[serverNumber]):]...),
		LeaderCommit: rf.commitedIndex,
	}
}
func (rf *Raft) checkAppendEntriesReplyStatus(serverNumber int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm { //higher term
		rf.setFollower(reply.Term)
	}
	if reply.Success {
		rf.matchIndex[serverNumber] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[serverNumber] = rf.matchIndex[serverNumber] + 1

		rf.updateCommitIndex()
		rf.updateAppliedIndex()

	} else {
		if reply.LastCommit != -1 {
			rf.nextIndex[serverNumber]--
		}
		if rf.nextIndex[serverNumber] <= 0 {
			fmt.Println("error in Leader", rf.me, "append =>", serverNumber, " with rf.nextIndex[", serverNumber, "] ", rf.nextIndex[serverNumber])
			//	var argsNew AppendEntriesArgs
			//		var replyNew AppendEntriesReply
			//	if rf.sendAppendEntriesViaServer(serverNumber, &argsNew, &replyNew) == true {
			//		rf.checkAppendEntriesReplyStatus(serverNumber, argsNew, reply)
			//	}
		}
	}

	return true
}
func (rf *Raft) sendAppendEntriesViaServer(serverNumber int) (*AppendEntriesArgs, *AppendEntriesReply, bool) {
	if rf.Status != LEADER {
		return nil, nil, false
	}
	reply := &AppendEntriesReply{}
	args := rf.getAppendEntriesArgsViaServer(serverNumber)
	if rf.Status != LEADER {
		return nil, nil, false
	}

	work := rf.sendAppendEntries(serverNumber, args, reply)
	if rf.Status != LEADER || work == false {
		return nil, nil, false
	}
	return args, reply, true
}

//heartBeats send heartBeats
func (rf *Raft) heartBeats() {
	var wg sync.WaitGroup
	for serverNumber := range rf.peers {
		if serverNumber != rf.me {
			wg.Add(1)
			go func(serverNumber int) {
				defer wg.Add(-1)
				args, reply, success := rf.sendAppendEntriesViaServer(serverNumber)
				if success == true {
					rf.checkAppendEntriesReplyStatus(serverNumber, args, reply)
				}

			}(serverNumber)
		}
	}
	wg.Wait()
}

func (rf *Raft) startServer() { //处理状态问题,
	heartbeatTimer := time.Duration(50) * time.Millisecond

	go func() { //监听 append entries 和 request vote 管理timer
		for {
			followerTimer := time.Duration(300+rand.Intn(300)) * (time.Millisecond)
			candidateTimer := time.Duration(300+rand.Intn(300)) * (time.Millisecond)
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
					case <-rf.getAppendEntrieschan: //decover current leader
					case <-rf.getRequestVotechan:
					case <-time.After(candidateTimer):
						rf.setCandidate()
					}
				}
			case LEADER:
				{
					go rf.heartBeats()
					time.Sleep(heartbeatTimer)
					if rf.Debug {
						//		fmt.Println("Leader ", rf.me, "end send heartBeats")
					}
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
	rf.Debug = false
	if rf.Debug {
		fmt.Printf("init server -----%d \n\n\n\n", rf.me)
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
