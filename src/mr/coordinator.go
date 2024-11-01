package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// nfiles: number of input files
// nReduce: number of reduce tasks
// map_cnt: number of available map tasks
// reduce_cnt: number of available reduce tasks
// map_wait_cnt: number of wait tasks still in progress
// reduce_wait_cnt: number of reduce tasks still in progress
// done: all tasks are done
// task_states []int: array of task_state (see below)

var mutex sync.Mutex

type Coordinator struct {
	// Your definitions here.
	filenames       []string
	nfiles          int
	nReduce         int
	map_cnt         int
	reduce_cnt      int
	map_wait_cnt    int
	reduce_wait_cnt int
	done            bool
	task_states     []task_state
}

// isMap: true if the task is Map, false if the task is Reduce
// state: 0 if the task is idle (unprocessed - no worker assigned yet), 1 if the task is in progress, 2 if the task's been completed.
// if a worker failed / is irresponsive, re-execute all its in-progress and completed MAP task on a different worker, and its in-progress REDUCE task state to idle.
// Completed MAP tasks are re-executed because each worker has its own local on-disk output files which are inaccessible because the worker has failed.
// Completed REDUCE tasks don't have to be re-executed as their output is stored in a global file system.
type task_state struct {
	isMap        bool
	state        int
	workerStatus int
}

var debug bool = false

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// called when a Map task is finished. Decrement the count, and check for Done.
func (c *Coordinator) SetMapDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	mutex.Lock()
	defer mutex.Unlock()

	if c.task_states[args.TaskID].state == 1 {
		c.task_states[args.TaskID].state = 2
		c.map_wait_cnt -= 1
	}

	if c.map_cnt == 0 && c.map_wait_cnt == 0 && c.reduce_cnt == 0 && c.reduce_wait_cnt == 0 {
		// All tasks are done. Tell the workers to terminate
		c.done = true
		reply.Done = true
	}
	return nil
}

// called when a Reduce task is finished. Decrement the count, and check for Done.
func (c *Coordinator) SetReduceDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	mutex.Lock()
	defer mutex.Unlock()

	// convert reduce task's ID to its corresponding index in the task_states array
	var id int = args.TaskID + c.nfiles
	if c.task_states[id].state == 1 {
		c.task_states[id].state = 2
		c.reduce_wait_cnt -= 1
	}

	if c.map_cnt == 0 && c.map_wait_cnt == 0 && c.reduce_cnt == 0 && c.reduce_wait_cnt == 0 {
		// All tasks are done. Tell the workers to terminate
		c.done = true
		reply.Done = true
	}
	return nil
}

// right now the coordinator only gives out map tasks
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	mutex.Lock()
	defer mutex.Unlock()
	if debug {
		fmt.Printf("%d %d %d %d\n", c.map_cnt, c.map_wait_cnt, c.reduce_cnt, c.reduce_wait_cnt)
	}
	if c.map_cnt > 0 { // assign Map task
		for i := 0; i < c.nfiles; i++ { // try to assign an idle map task
			if c.task_states[i].state == 0 {
				reply.Filename = c.filenames[i]
				reply.IsMap = true
				reply.TaskID = i
				reply.Nreduce = c.nReduce
				reply.Done = false
				reply.Nfiles = c.nfiles

				c.map_cnt -= 1
				c.map_wait_cnt += 1
				c.task_states[i].state = 1
				go func(id int) {
					time.Sleep(10 * time.Second)
					mutex.Lock()
					if c.task_states[id].state == 1 { // worker timeout, set status to failed
						c.task_states[id].state = 0
						c.map_cnt += 1
						c.map_wait_cnt -= 1
					}
					mutex.Unlock()
				}(i)
				/* go func() {
					for c.task_states[i].workerStatus == 0 {
						mutex.Lock()
						if c.task_states[i].workerStatus == -1 { // worker timeout

						} else {

						}
						mutex.Unlock()
					}
				}() */

				return nil
			}
		}

	} else if c.reduce_cnt > 0 { // assign Reduce task
		if c.map_wait_cnt > 0 {
			return errors.New("waiting for all map tasks to finish before assigning reduce tasks")
		}
		for i := c.nfiles; i < c.nfiles+c.nReduce; i++ { // try to assign a reduce task
			if c.task_states[i].state == 0 {
				reply.Filename = fmt.Sprintf("mr-out-%d", i-c.nfiles)
				reply.IsMap = false
				reply.TaskID = i - c.nfiles
				reply.Nreduce = c.nReduce
				reply.Done = false
				reply.Nfiles = c.nfiles

				c.reduce_cnt -= 1
				c.reduce_wait_cnt += 1
				c.task_states[i].state = 1
				go func(id int) {
					time.Sleep(10 * time.Second)
					mutex.Lock()
					if c.task_states[id].state == 1 { // worker timeout, set status to failed
						c.task_states[id].state = 0
						c.reduce_cnt += 1
						c.reduce_wait_cnt -= 1
					}
					mutex.Unlock()
				}(i)

				return nil
			}
		}

	} else if c.map_wait_cnt > 0 || c.reduce_wait_cnt > 0 {
		return errors.New("waiting for tasks to finish")
	}
	reply.Done = true
	c.done = true
	return errors.New("done")
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Lierrorsten("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, num_reduce int) *Coordinator {
	c := Coordinator{filenames: files, nfiles: len(files), map_cnt: len(files), nReduce: num_reduce, reduce_cnt: num_reduce}
	c.task_states = []task_state{}
	c.done = false
	for i := 0; i < c.nfiles; i++ {
		c.task_states = append(c.task_states, task_state{isMap: true, state: 0, workerStatus: 0})
	}
	for i := 0; i < num_reduce; i++ {
		c.task_states = append(c.task_states, task_state{isMap: false, state: 0, workerStatus: 0})
	}
	c.server()
	return &c
}
