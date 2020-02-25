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
	"github.com/clatisus/MIT6.824DS/src/labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "github.com/clatisus/MIT6.824DS/src/labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type ServerState int

const (
	FOLLOWER  ServerState = 0
	CANDIDATE ServerState = 1
	LEADER    ServerState = 2
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	sync.Mutex                     // Lock to protect shared access to this peer's state
	peers      []*labrpc.ClientEnd // RPC end points of all peers
	persister  *Persister          // Object to hold this peer's persisted state
	me         int                 // this peer's index into peers[]
	dead       int32               // set by Kill()

	// the paper's Figure 2
	status     ServerState
	expireTime time.Time

	// Persistent state on all servers
	// Updated on stable storage before responding to RPCs
	currentTerm int        // latest term server has seen (init to 0 on first boot, increases)
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         []LogEntry // log entries (first index is 1)

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed (init to 0, increases)
	lastApplied int // index of highest log entry applied to state machine (init to 0, increases)

	// Volatile state on leaders
	// Reinitialized after election
	nextIndex  []int // peer's index -> the next log entry index to send (leader -> last log index + 1)
	matchIndex []int // peer's index -> the highest log entry index known to be replicated (init to 0, increases)
}

// each entry contains command for state machine, and term when entry was received by leader
type LogEntry struct {
	Command interface{}
	Term    int
}

func (rf *Raft) GetState() (term int, isLeader bool) {
	rf.Lock()
	defer rf.Unlock()
	term = rf.currentTerm
	isLeader = rf.status == LEADER
	return
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

//
// Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2)
// Receiver implementation:
//   1. Reply false if term < currentTerm (§5.1)
//   TODO: 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
//   TODO: 3. If an existing entry conflicts with a new one (same index but different terms),
//      delete the existing entry and all that follow it (§5.3)
//   TODO: 4. Append any new entries not already in the log
//   TODO: 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Lock()
	defer rf.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.status = FOLLOWER
	// Rules for All Servers
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.refreshExpire()

	reply.Term = rf.currentTerm
	reply.Success = true
	return
}

//
// Invoked by candidates to gather votes
//
type RequestVoteArgs struct {
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// Receiver implementation:
//   1. Reply false if term < currentTerm (§5.1)
//   2. If votedFor is null or candidateId, and candidate’s log
//      is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.Lock()
	defer rf.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// Rules for All Servers
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = FOLLOWER
		rf.votedFor = -1 // prepare to vote for the requesting server
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogIndex := len(rf.log) - 1
		lastLogTerm := rf.log[len(rf.log)-1].Term
		// candidate's log is at least as up-to-date as receiver's log
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			rf.status = FOLLOWER
			rf.votedFor = args.CandidateId
			rf.refreshExpire()

			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			return
		}
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return
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
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
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
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// init
	rf.status = FOLLOWER
	rf.refreshExpire()

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1) // index start from 1, init with a placeholder

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.tick()

	return rf
}

func (rf *Raft) tick() {
	for !rf.killed() {
		func() {
			rf.Lock()
			defer rf.Unlock()
			if rf.expireTime.After(time.Now()) {
				time.Sleep(10 * time.Millisecond)
				return
			}
			switch rf.status {
			case FOLLOWER:
				rf.status = CANDIDATE
				fallthrough
			case CANDIDATE:
				rf.requestVote()
			case LEADER:
				rf.heartbeat()
			}
		}()
	}
}

// lock is hold by caller
func (rf *Raft) requestVote() {
	// On conversion to candidate, start election:
	// • Increment currentTerm
	rf.currentTerm++
	// • Vote for self
	rf.votedFor = rf.me
	// • Reset election timer
	rf.refreshExpire()

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	var votes int32 = 1

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// • Send RequestVote RPCs to all other servers
		go func(i int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &args, &reply)
			if !ok {
				return
			}
			rf.Lock()
			defer rf.Unlock()

			// Rules for All Servers
			// If RPC request or response contains term T > currentTerm:
			// set currentTerm = T, convert to follower (§5.1)
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.status = FOLLOWER
				rf.votedFor = -1 // reset to vote for another candidate
				return
			}
			// rf.currentTerm may not be the one when launches this goroutine
			// maybe second round requestVote increases rf.currentTerm
			if reply.Term == rf.currentTerm && reply.VoteGranted && rf.status == CANDIDATE {
				if int(atomic.AddInt32(&votes, 1)) > len(rf.peers)/2 {
					rf.status = LEADER
					rf.heartbeat()
				}
			}
		}(i)
	}
}

// lock is hold by caller
func (rf *Raft) heartbeat() {
	rf.refreshExpire()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			var args AppendEntriesArgs
			func() {
				rf.Lock()
				defer rf.Unlock()
				if rf.status != LEADER {
					return
				}
				args = AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
					Entries:      nil,
					LeaderCommit: rf.commitIndex,
				}
			}()
			// unlock to RPC
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(i, &args, &reply)
			if !ok {
				return
			}

			rf.Lock()
			defer rf.Unlock()
			// network partition, reply from other side's new elected leader
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.status = FOLLOWER
				rf.votedFor = -1
				rf.refreshExpire()
			}
		}(i)
	}
}

func (rf *Raft) refreshExpire() {
	switch rf.status {
	case FOLLOWER, CANDIDATE:
		rf.expireTime = time.Now().Add(time.Duration(200+rand.Intn(100)) * time.Millisecond)
	case LEADER:
		// heartbeat
		rf.expireTime = time.Now().Add(100 * time.Millisecond)
	}
}
