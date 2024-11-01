# Distributed-Systems-Lab-MIT-6.5840 2024 Labs by Tin Vuong

This repo contains my implementation for the 2024 labs of this [course](https://pdos.csail.mit.edu/6.824/index.html)

Forked from another student's [source](https://github.com/vasukalariya/MIT-Distributed-Systems-Lab-6.824-6.5840) only to get the setup source code. All of the implementations are of my own.

Learned the concepts through the [course content](https://pdos.csail.mit.edu/6.824/schedule.html) and the official Go documentation.

- [x] Lab 1: Map Reduce

Takeaways:
 - The reply argument in the RPC call MUST be initialized to default values.
 - Check the capitalization of struct member names and methods for visibility scopes.
 - Use some sort of exit notification from coordinator when worker pings coordinator and there are no tasks available.
 - You should use go routines to track the progress of tasks. Hint: Sleep and Mutex.
 - Goroutines don't exit even if their "parent" (Go doesn't have a notion of a parent) returns. This means that a goroutine will continue executing even if the caller exits. The only exception is that when `main` exits, all goroutines will exit.



