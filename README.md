# Distributed-Systems-Lab-MIT-6.5840 2024 Labs by Tin Vuong

This repo contains my implementation for the 2024 labs of this [course](https://pdos.csail.mit.edu/6.824/index.html)

Forked from another student's [source](https://github.com/vasukalariya/MIT-Distributed-Systems-Lab-6.824-6.5840) only to get the setup source code. All of the implementations are of my own.

Learned the concepts through the [course content](https://pdos.csail.mit.edu/6.824/schedule.html) and the official Go documentation.


# Lab 1: Map Reduce

## Status
Finished


## Dev notes

 - The reply argument in the RPC call MUST be initialized to default values.
 - Check the capitalization of struct member names and methods for visibility scopes.
 - Use some sort of exit notification from coordinator when worker pings coordinator and there are no tasks available.
 - You should use go routines to track the progress of tasks. Hint: Sleep and Mutex.
 - Goroutines don't exit even if their "parent" (Go doesn't have a notion of a parent) returns. This means that a goroutine will continue executing even if the caller exits. The only exception is that when `main` exits, all goroutines will exit.



# Lab 3: Raft

Goal: to implement the Raft consensus algorithm presented in this [paper](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf).

## Status
Finished 3A and 3B tests, including:

- Everything related to leader elections
- Recovering from leader and peer failures
- Committing logs
- Sending heartbeats


## Dev notes


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
    args.Entas $k \rightarrow \infty$:ries = nil
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

### Heartbeat

When sending a heartbeat, we need to set `PrevLogIndex` to `len(rf.log)` since an `PrevLogIndex` stands for the index of the log immediately preceding the new ones. But since the new entries are empty for a heartbeat, it means we're at the end of the log. 

This will help solve the case of a rejoined leader:

1. There are 3 servers. Leader is **0**. Initially, all 3 have `[0]` in their logs with term 1.
2. Then **Leader 0** disconnects from the cluster. **1** is elected to be next leader of term 2.
3. **0** still thinks it's connected to the server, and begins replicating `[1,2,3]` in its log with term 1.
4. **1** and **2** replicate `[3]` with term 2. Now the logs look like:
    - **0**: `[0,1,2,3]`.
    - **1**: `[0,3]`.
    - **2**: `[0,3]`.
5. **Leader 1** crashses, and **0** rejoins.
6. **2** should become the new leader, since its entry `3` at index `1` has term **2**, while the same entry of server **0** has term **1**.
7. Now, we need to replicate **2**'s log to **0**. **Leader 2** sends a heartbeat to **0**. But how does **0** know that its entry at index `1` is invalid?
8. => Even though **Leader 2** is sending an empty entry, it can still sets its argument `PrevLogIndex` and `PrevLogTerm` to the last one in **2**'s log (a.k.a `3` with term 2). This will force **0** to discard everything from its entry `1` at index `1` (with term 1) to the end.


