package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "errors"
import "sync"
import "fmt"

type Coordinator struct {
	// Your definitions here.
	filenames []string
	nfiles int
	nReduce int
	map_cnt int
	reduce_cnt int
	done bool
	mutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


// right now the coordinator only gives out map tasks
func (c *Coordinator) RequestTask (args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.map_cnt > 0 {	// assign Map task
		reply.Filename = c.filenames[c.map_cnt - 1]
		reply.IsMap = true
		c.map_cnt -= 1
		return nil

	} else if c.reduce_cnt > 0 {	// assign Reduce task
		reply.Filename = fmt.Sprintf("mr-out-%d", int(c.reduce_cnt));
		reply.IsMap = false 
		c.reduce_cnt -= 1
		return nil

	} else {
		return errors.New("No tasks available\n");
	}
	
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{filenames: files, nfiles: cap(files), map_cnt: cap(files), reduce_cnt: nReduce}

	c.server()
	return &c
}
