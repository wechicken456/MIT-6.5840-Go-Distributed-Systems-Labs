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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type AppendEntriesArgs struct {
	// Arguments
	Term              int // leader's term, also used as a return value to help the leader drop to follower should another server is more up-to-date
	LeaderId          int
	PrevLogIndex      int        // index of log entry immediate preceding the new ones to be appended
	PrevLogTerm       int        // term of PrevLogIndex entry
	Entries           []LogEntry // empty for heartbeat
	EntriesLen        int        // len of Entries
	LeaderCommitIndex int        // leader's commitIndex (check struct Raft below)
}

type AppendEntriesReply struct {
	// Results
	Term    int  // Server's term. Used as return value to help the leader drop to follower should another server is more up-to-date
	Success bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
}

type LogEntry struct {
	Command   interface{}
	TermIndex int // term when entry was received by the leader
}

var Follower int32 = 0
var Candidate int32 = 1
var Leader int32 = 2
var voteCnt int = 0
var electionTimeoutDur = 500 // in milliseconds
var RetryTimeout = time.Duration(100) * time.Millisecond

// Election timeout to check a leader election should be started.
var electionTimeout time.Duration

// RNG for each server, use server id as seed
var randSrc *rand.Rand

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	lastElectionTime time.Time
	state            int32 // 0 for leader, 1 for candidate, 2 for leader

	// persistent state on all servers
	currentTerm int        // last term server has seen (init'ed to 0, increases monotically)
	votedFor    int        // candidate ID we voted for in the current term (nil if none)
	log         []LogEntry //log entries; each entry contains {command, term when entry was received by the leader}
	LeaderId    int

	// volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// volatile state on leaders
	nextIndex  []int // log[i] = index of the next log entry to send to server i.
	matchIndex []int // matchIndex[i] = index of highest log entry replicated on server i.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int //term of candidate’s last log entry (§5.4
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	// False if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if rf.currentTerm > args.Term || args.PrevLogIndex >= len(rf.log) ||
		args.PrevLogTerm != rf.log[args.PrevLogIndex].TermIndex {
		reply.Success = false
	} else {
		reply.Success = true
		reply.Term = args.Term

		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.LeaderId = args.LeaderId
		voteCnt = 1
		rf.lastElectionTime = time.Now() // reset election timeout
		DPrintf("Debug, Server %d, State %d: received AppendEntriesRPC from %d, convert to follower \n", rf.me, rf.state, args.LeaderId)
		rf.mu.Unlock()
		return
	}
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntriesRPCToPeer(peerId int) {

	rf.mu.Lock()
	logLen := len(rf.log)
	args := &AppendEntriesArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		PrevLogTerm:       rf.log[logLen-1].TermIndex,
		PrevLogIndex:      logLen - 1,
		LeaderCommitIndex: rf.commitIndex,
		Entries:           nil,
		EntriesLen:        0}
	reply := &AppendEntriesReply{}
	rf.mu.Unlock()

	DPrintf("Debug, Leader %d: SENDING AppendEntries RPC to peer %d \n", rf.me, peerId)
	ok := rf.peers[peerId].Call("Raft.AppendEntries", args, reply)
	DPrintf("Debug, Leader %d: FINISHED AppendEntries RPC to peer %d \n", rf.me, peerId)

	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	if ok {
		if reply.Term > rf.currentTerm {
			rf.state = Follower
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.LeaderId = -1
			voteCnt = 1
			rf.lastElectionTime = time.Now() // reset election timeout

		}
		DPrintf("Debug, Leader %d: SUCCEEDED AppendEntries RPC to peer %d \n", rf.me, peerId)
	} else {
		DPrintf("Debug, Leader %d: FAILED AppendEntries RPC to peer %d \n", rf.me, peerId)
		go func() {
			time.Sleep(time.Duration(100) * time.Millisecond)
			rf.sendAppendEntriesRPCToPeer(peerId)
		}()
	}
	rf.mu.Unlock()

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false

	logLen := len(rf.log)
	if (rf.log[logLen-1].TermIndex > args.LastLogTerm) ||
		(rf.log[logLen-1].TermIndex == args.LastLogTerm && logLen-1 > args.LastLogIndex) {
		// this server has longer log, under same term
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("Debug, Candidate %d: DENIED vote to peer %d \n", rf.me, args.CandidateId)
		return
	}

	if rf.currentTerm <= args.Term {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			DPrintf("Debug, Candidate %d: GRANTED vote to peer %d \n", rf.me, args.CandidateId)
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.state = Follower // can only vote for 1 candidate at a time, convert to follower to prevent myself from becoming a candidate
		}
		rf.currentTerm = args.Term
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) requestVoteFromPeers(peerId int) {

	rf.mu.Lock()
	logLen := len(rf.log)
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  rf.log[logLen-1].TermIndex,
		LastLogIndex: logLen - 1,
	}
	reply := &RequestVoteReply{}
	rf.mu.Unlock()

	ok := rf.peers[peerId].Call("Raft.RequestVote", args, reply)

	DPrintf("Debug, Candidate %d: BEFORE grabbing RequestVote lock for peer %d\n", rf.me, peerId)
	rf.mu.Lock()
	DPrintf("Debug, Candidate %d: AFTER grabbing RequestVote lock for peer %d\n", rf.me, peerId)
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}

	if ok {
		if reply.VoteGranted {
			voteCnt++
			DPrintf("Debug, Candidate %d: RECEIVED vote from peer %d\n", rf.me, peerId)
			if voteCnt > int(len(rf.peers)/2) {
				DPrintf("Debug, Candidate %d: got majority \n", rf.me)
				rf.state = Leader
				rf.votedFor = -1
				rf.LeaderId = rf.me
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = rf.log[len(rf.log)-1].TermIndex
				}
			}
		} else { // stop election

			if rf.currentTerm < reply.Term {
				rf.state = Follower
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.LeaderId = -1
				voteCnt = 1
				rf.persist()
				DPrintf("Debug, Candidate %d: didn't get vote from peer %d. Reseting to FOLLOWER\n", rf.me, peerId)
			}
		}
	} else { // call failed, retry
		DPrintf("Debug, Candidate %d: failed reach peer %d for vote \n", rf.me, peerId)
	}
	rf.mu.Unlock()
}

func (rf *Raft) replicateLogs() {
	heartBeatDur := time.Duration(100) * time.Millisecond
	for !rf.killed() {
		for i := 0; i < len(rf.peers); i++ {
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			if i == rf.me {
				rf.mu.Unlock()
				continue
			}
			rf.mu.Unlock()
			go rf.sendAppendEntriesRPCToPeer(i)
		}
		time.Sleep(heartBeatDur)
	}
}

func (rf *Raft) startElection() {

	DPrintf("Debug, Candidate %d: timed out, calling new election \n", rf.me)
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.LeaderId = -1
	voteCnt = 1

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.requestVoteFromPeers(i)
		}
	}
}

func (rf *Raft) ticker() {

	voteCnt = 1
	electionTimeout = time.Duration(int64(electionTimeoutDur)+(randSrc.Int63()%800)) * time.Millisecond
	rf.lastElectionTime = time.Now()
	DPrintf("Debug, election timeout: %v\n", electionTimeout)

	for !rf.killed() {
		rf.mu.Lock()
		DPrintf("Debug, Server %d: ticking. State: %d\n", rf.me, rf.state)
		if rf.state == Leader { // Leader
			rf.mu.Unlock()
			rf.replicateLogs()
		} else {
			rf.mu.Unlock()
			if time.Since(rf.lastElectionTime) > electionTimeout {
				DPrintf("Debug, Candidate %d: timed out, calling new election \n", rf.me)
				rf.lastElectionTime = time.Now()
				electionTimeout = time.Duration(int64(electionTimeoutDur)+(randSrc.Int63()%500)) * time.Millisecond

				rf.mu.Lock()
				// 1st clause: if we haven't voted for anyone, and a leader hasn't been elected
				// 2nd clause: if we have elected ourselves, but we're not the leader (leader check was in the outer if)
				if (rf.votedFor == -1 && rf.LeaderId == -1) || rf.votedFor == rf.me {
					rf.mu.Unlock()
					rf.startElection()
				} else {
					rf.mu.Unlock()
				}

				rf.mu.Lock()
				if rf.state != Leader { // reset after each election/term, as now we're just a follower
					rf.LeaderId = -1
					rf.votedFor = -1
				}
				rf.mu.Unlock()
			}
		}
		time.Sleep(time.Duration(50+randSrc.Int63()%250) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = Follower
	rf.dead = 0
	rf.votedFor = -1
	rf.LeaderId = -1
	rf.log = append(rf.log, LogEntry{TermIndex: 0})
	rf.nextIndex = make([]int, len(peers))

	// Your initialization code here (3A, 3B, 3C).
	randSrc = rand.New(rand.NewSource(int64(rf.me))) // a different seed for each server

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
