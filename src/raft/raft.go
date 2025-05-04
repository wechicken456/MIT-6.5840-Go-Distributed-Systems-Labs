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
	muApplyCh sync.Mutex          // Lock to protect applyCh. Separate from `mu` to improve efficiency
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	lastElectionTime time.Time
	state            int32 // 0 for leader, 1 for candidate, 2 for leader
	applyCh          chan ApplyMsg

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
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || rf.killed() {
		return -1, -1, false
	}

	//DPrintf("Leader %d: RECEIVED command %v, logLen: %d\n", rf.me, command, len(rf.log))
	logEntry := LogEntry{Command: command, TermIndex: rf.currentTerm}
	rf.log = append(rf.log, logEntry)
	rf.nextIndex[rf.me] = len(rf.log)
	rf.matchIndex[rf.me] = len(rf.log) - 1
	logLen := len(rf.log)

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.sendAppendEntriesToPeers(i, false, rf.nextIndex[i])
		}
	}
	return logLen - 1, rf.currentTerm, true
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

func (rf *Raft) commitLog() {
	for !rf.killed() {
		var entryToApply ApplyMsg

		rf.muApplyCh.Lock()
		if rf.commitIndex > rf.lastApplied {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				entryToApply = ApplyMsg{
					CommandValid: true,
					CommandIndex: i,
					Command:      rf.log[i].Command,
				}
				rf.applyCh <- entryToApply
			}
			rf.lastApplied = rf.commitIndex
		}

		rf.muApplyCh.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

/* Only called by the leader to propagate the log */
func (rf *Raft) sendAppendEntriesToPeers(peerId int, isEmpty bool, sendEntriesFromIndex int) {
	//DPrintf("Leader %d: FINISHED AppendEntries RPC to peer %d \n", rf.me, peerId)

	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	args := &AppendEntriesArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LeaderCommitIndex: rf.commitIndex,
	}
	args.PrevLogIndex = sendEntriesFromIndex - 1           // index of log entry immediately preceding the new ones to be appended
	args.PrevLogTerm = rf.log[args.PrevLogIndex].TermIndex // term of the PrevLogIndex entry

	/* isEmpty is True if the leader is sending a heartbeat */
	if isEmpty || rf.nextIndex[peerId] == len(rf.log) {
		args.Entries = nil
		args.EntriesLen = 0
	} else {
		args.Entries = rf.log[sendEntriesFromIndex:]
		args.EntriesLen = len(args.Entries)
	}

	DPrintf("Leader %d: SENDING AppendEntries RPC to peer %d: {PrevLogIndex: %d, PrevLogTerm: %d, Entries: %v, LeaderCommitIdx: %d}\n", rf.me, peerId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommitIndex)
	reply := &AppendEntriesReply{}
	rf.mu.Unlock()

	// this call will wait for a reply from the peer
	// so we need to release the lock before this call so it doesn't block other threads
	ok := rf.peers[peerId].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		DPrintf("Leader %d: REACHED AppendEntries RPC to peer %d \n", rf.me, peerId)
		if reply.Term > rf.currentTerm { // server has larger term, convert ourselves to follower
			rf.state = Follower
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.LeaderId = -1
			voteCnt = 1
			rf.lastElectionTime = time.Now() // reset election timeout
		} else {
			if reply.Success { // follower accepted the new entry, increment their indices
				// If another thread already updated nextIndex, we shouldn't change it unless we have more entries
				if args.PrevLogIndex+args.EntriesLen > rf.matchIndex[peerId] {
					rf.nextIndex[peerId] = args.PrevLogIndex + args.EntriesLen + 1
					rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1
				}

				DPrintf("LEADER %d, peer %d, matchIndex[%d] = %d, nextIndex[%d] = %d\n", rf.me, peerId, peerId, rf.matchIndex[peerId], peerId, rf.nextIndex[peerId])

				// case this follower was "late" for the commit. This could happen if the commit message was sent while this follower was disconnected.
				// then do nothing
				if rf.commitIndex >= rf.matchIndex[peerId] {
					return
				}

				// Everything below is for committing the log
				// if there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
				// set commitIndex = N
				// (we only count replicas within the same term, not previous terms, see the Raft paper's safety TFHMPargument for more)
				newCommitIndex := len(rf.log) + 1
				cnt := 0 // how many peers have replicated logs higher than commitIndex, including us
				for i := 0; i < len(rf.peers); i++ {
					if rf.matchIndex[i] > rf.commitIndex && rf.log[rf.matchIndex[i]].TermIndex == rf.currentTerm {
						cnt++
						newCommitIndex = min(newCommitIndex, rf.matchIndex[i])
					}
				}
				if newCommitIndex == len(rf.log)+1 { // can't commit anything right now
					return
				}

				DPrintf("LEADER %d, peer %d, k = %d, v = %d\n", rf.me, peerId, newCommitIndex, cnt)
				if newCommitIndex > rf.commitIndex && cnt > int(len(rf.peers)/2) && rf.log[newCommitIndex].TermIndex == rf.currentTerm {
					rf.commitIndex = newCommitIndex
					for i := 0; i < len(rf.peers); i++ {
						if i != rf.me {
							go rf.sendHeartBeatToPeer(i)
						}
					}
					DPrintf("LEADER %d, COMMITTED index %d, k = %d, v = %d\n", rf.me, rf.commitIndex, newCommitIndex, cnt)
				}

			} else {
				// follower log doesn’t contain an entry at PrevLogIndex whose term matches PrevLogTerm
				// decrement index and retry
				DPrintf("Leader %d: REDUCING nextIndex %d -> %d, AppendEntries RPC to peer %d \n", rf.me, rf.nextIndex[peerId], rf.nextIndex[peerId]-1, peerId)
				// there could be concurrent threads decrementing nextIndex,
				// since every Raft peer has the same 0th log, stop here before going OOB.
				if rf.nextIndex[peerId] <= 0 {
					return
				}

				// only update if these are equal.
				// If they are not, another thread updated it while we were waiting for our (this thread's) response, so we shouldn't change it and should return.
				//if (rf.nextIndex[peerId]-1 == args.PrevLogIndex) && (rf.log[rf.nextIndex[peerId]-1].TermIndex == args.PrevLogTerm) {
				rf.nextIndex[peerId] = max(1, min(rf.nextIndex[peerId]-1, len(rf.log)))
				go rf.sendAppendEntriesToPeers(peerId, false, rf.nextIndex[peerId])
				//}
			}
		}
	} else { // call failed, follower might not be up. Retry.
		DPrintf("Leader %d: FAILED AppendEntries RPC to peer %d. RETRYING... \n", rf.me, peerId)
		if args.EntriesLen > 0 { // don't resend a heartbeat
			go func() {
				time.Sleep(time.Duration(150) * time.Millisecond)
				rf.sendAppendEntriesToPeers(peerId, false, sendEntriesFromIndex)
			}()
		}
	}
}

/* AppendEntries RPC handler. Executed only by followers x*/
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if we have greater term, reject the request
	if rf.currentTerm > args.Term {
		DPrintf("INNNNNNN00000\n")
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		// must check this because a rejoined leader might think it's still a leader until it
		// receives an entry from the current leader having a greater term
		rf.state = Follower
		rf.votedFor = -1
		rf.LeaderId = args.LeaderId
	}

	rf.lastElectionTime = time.Now() // reset election timeout
	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		DPrintf("Server %d: INNNNNNN11111, PrevLogIndex: %d, logLen: %d\n", rf.me, args.PrevLogIndex, len(rf.log))
	} else if args.PrevLogTerm != rf.log[args.PrevLogIndex].TermIndex {
		DPrintf("Server %d: INNNNNNN22222\n", rf.me)
		reply.Success = false
		rf.log = rf.log[:args.PrevLogIndex] // mismatch entries at the given index, remove all entries from our log at this index til the end.
	} else {
		reply.Success = true
		reply.Term = args.Term

		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.LeaderId = args.LeaderId

		DPrintf("Server %d: INNNNNNN33333,  logLen: %v\n", rf.me, len(rf.log))
		for i := 0; i < args.EntriesLen; i++ {
			idx := args.PrevLogIndex + i + 1

			if idx >= len(rf.log) {
				rf.log = append(rf.log, args.Entries[i])
			} else {
				if rf.log[idx].TermIndex == args.Entries[i].TermIndex {
					continue
				} else {
					rf.log[idx] = args.Entries[i]
				}
			}
		}
		DPrintf("Server %d: INNNNNNN33333, logLen: %v\n", rf.me, len(rf.log))
		// commit new log entries if possible
		if args.LeaderCommitIndex > rf.commitIndex {
			newCommitIndex := min(args.LeaderCommitIndex, len(rf.log)-1)
			rf.commitIndex = newCommitIndex
			DPrintf("Server %d: received AppendEntriesRPC from %d, commitIndex: %d, logLen: %d \n", rf.me, rf.LeaderId, rf.commitIndex, len(rf.log))
		}
		voteCnt = 1
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendHeartBeatToPeer(peerId int) {

	// rf.mu.Lock()
	// logLen := len(rf.log)
	// args := &AppendEntriesArgs{
	// 	Term:              rf.currentTerm,
	// 	LeaderId:          rf.me,
	// 	PrevLogTerm:       rf.log[logLen-1].TermIndex,
	// 	PrevLogIndex:      logLen - 1,
	// 	LeaderCommitIndex: rf.commitIndex,
	// 	Entries:           nil,
	// 	EntriesLen:        0}
	// reply := &AppendEntriesReply{}
	// rf.mu.Unlock()
	rf.sendAppendEntriesToPeers(peerId, true, 1)
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
		DPrintf("Candidate %d: DENIED vote to peer %d \n", rf.me, args.CandidateId)
		return
	}

	if rf.currentTerm <= args.Term {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			DPrintf("Candidate %d: GRANTED vote to peer %d \n", rf.me, args.CandidateId)
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

	DPrintf("Candidate %d: BEFORE grabbing RequestVote lock for peer %d\n", rf.me, peerId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Candidate %d: AFTER grabbing RequestVote lock for peer %d\n", rf.me, peerId)
	if rf.state != Candidate {
		return
	}

	if ok {
		if reply.VoteGranted {
			voteCnt++
			DPrintf("Candidate %d: RECEIVED vote from peer %d\n", rf.me, peerId)
			if voteCnt > int(len(rf.peers)/2) { // won the election
				DPrintf("Candidate %d: got majority \n", rf.me)
				rf.state = Leader
				rf.votedFor = -1
				rf.LeaderId = rf.me
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0
				}
			}
		} else {

			if rf.currentTerm < reply.Term { // if the peer requesting vote from us has higher term, we must vote for them and convert to follower
				rf.state = Follower
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.LeaderId = -1
				voteCnt = 1
				rf.persist()
				//DPrintf("Candidate %d: didn't get vote from peer %d. Reseting to FOLLOWER\n", rf.me, peerId)
			}
		}
	} else { // call failed, retry
		DPrintf("Candidate %d: failed reach peer %d for vote \n", rf.me, peerId)
		go func() {
			time.Sleep(time.Duration(150) * time.Millisecond)
			rf.requestVoteFromPeers(peerId)
		}()
	}
}

func (rf *Raft) sendHeartBeats() {
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
			go rf.sendHeartBeatToPeer(i)
		}
		time.Sleep(heartBeatDur)
	}
}

func (rf *Raft) startElection() {

	DPrintf("Candidate %d: timed out, calling new election \n", rf.me)
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
	rf.state = Follower
	electionTimeout = time.Duration(int64(electionTimeoutDur)+(randSrc.Int63()%500)) * time.Millisecond
	rf.lastElectionTime = time.Now()
	DPrintf("election timeout: %v\n", electionTimeout)

	go rf.commitLog()
	for !rf.killed() {
		rf.mu.Lock()
		DPrintf("Server %d: ticking. State: %d. Log len: %d. commitIndex: %d\n", rf.me, rf.state, len(rf.log), rf.commitIndex)
		if rf.state == Leader { // Leader
			rf.mu.Unlock()
			rf.sendHeartBeats()
		} else {
			rf.mu.Unlock()
			if time.Since(rf.lastElectionTime) > electionTimeout {
				DPrintf("Candidate %d: timed out, calling new election \n", rf.me)
				rf.lastElectionTime = time.Now()
				electionTimeout = time.Duration(int64(electionTimeoutDur)+(randSrc.Int63()%500)) * time.Millisecond

				rf.mu.Lock()
				// 1st clause: if we haven't voted for anyone, and a leader hasn't been elected
				// 2nd clause: if we have elected ourselves, but we're not the leader (leader check was in the outer if)
				if (rf.votedFor == -1 && rf.LeaderId == -1) || rf.votedFor == rf.me {
					rf.startElection()
				}
				if rf.state != Leader { // reset after each election/term, as now we're just a follower
					rf.LeaderId = -1
					//rf.votedFor = -1
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
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	randSrc = rand.New(rand.NewSource(int64(rf.me))) // a different seed for each server

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
