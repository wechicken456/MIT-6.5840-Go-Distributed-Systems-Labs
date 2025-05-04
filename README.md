# Distributed-Systems-Lab-MIT-6.5840 2024 Labs by Tin Vuong

This repo contains my implementation for the 2024 labs of this [course](https://pdos.csail.mit.edu/6.824/index.html)

Forked from another student's [source](https://github.com/vasukalariya/MIT-Distributed-Systems-Lab-6.824-6.5840) only to get the setup source code. All of the implementations are of my own.

Learned the concepts through the [course content](https://pdos.csail.mit.edu/6.824/schedule.html) and the official Go documentation.


## Lab 1: Map Reduce

Takeaways:
 - The reply argument in the RPC call MUST be initialized to default values.
 - Check the capitalization of struct member names and methods for visibility scopes.
 - Use some sort of exit notification from coordinator when worker pings coordinator and there are no tasks available.
 - You should use go routines to track the progress of tasks. Hint: Sleep and Mutex.
 - Goroutines don't exit even if their "parent" (Go doesn't have a notion of a parent) returns. This means that a goroutine will continue executing even if the caller exits. The only exception is that when `main` exits, all goroutines will exit.



## Lab 2: Raft

### Dev notes


When sending an AppendEntriesRPC to a follower using `sendAppenEntriesToPeers(peerId int, isEmpty bool)`, we can't just assign `PrevLogIndex = rf.nextIndex[peerId] - 1`:

```Go
args := &AppendEntriesArgs{
    Term:              rf.currentTerm,
    LeaderId:          rf.me,
    LeaderCommitIndex: rf.commitIndex,
}
args.PrevLogIndex = rf.nextIndex[peerId] - 1           // index of log entry immediately preceding the new ones to be appended
args.PrevLogTerm = rf.log[args.PrevLogIndex].TermIndex // term of the PrevLogIndex entry

/* isEmpty is True if the leader is sending a heartbeat */
if isEmpty || rf.nextIndex[peerId] == len(rf.log) {
    args.Entries = nil
    args.EntriesLen = 0
} else {
    args.Entries = rf.log[args.PrevLogIndex+1:]
    args.EntriesLen = len(args.Entries)
}
```

At first, I thought this maade sense since `rf.nextIndex[peerId]` contains the index of the next entry to send to that peer.

However, consider the following scenario:

1. Leader has log entries [0,1,2,3]
2. Two concurrent `Start()` calls add entries 4 and 5 to the leader. 
3. Two AppendEntries RPCs are sent to follower `F`:
    - RPC1: PrevLogIndex=3, entries=[4]
    - RPC2: PrevLogIndex=3, entries=[5]
    - At this point, `rf.nextIndex[peerId] = 3`. 
4. If RPC2 arrives first, then `F` will check that the log entry at `PrevLogIndex = 3` is correct, so `F` appends `5` before `4` arrives.

[+] Solution: we need to add another argument to `senAppendEntriesToPeer` to take an argument `sendEntriesFromIndex`. Then, the leader will send to that peer all entries starting from `sendEntriesFromIndex` until the end. 




4. RPC2 arrives at follower `F` first, response comes back first
5. The response updates `nextIndex[F]` to 6 and `matchIndex[F]` to 5
6. RPC1 response arrives later
7. Your check args.PrevLogIndex+args.EntriesLen >= rf.nextIndex[peerId] becomes 3+1 >= 6, which is false. So you don't update matchIndex for entry 4, but it was actually replicated. This causes the commit logic to consider entry 4 as unreplicated
