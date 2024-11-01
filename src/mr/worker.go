package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMap(mapf func(string, string) []KeyValue, reply *RequestTaskReply) {
	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("Map: cannot open %v", reply.Filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("Map: cannot read %v", reply.Filename)
	}
	file.Close()
	kva := mapf(reply.Filename, string(content))

	// write the intermediate key/value pairs into a temporaray file
	// map of reduce worker ID -> corresponding json encoder
	m := make(map[int]*json.Encoder)
	for i := 0; i < reply.Nreduce; i++ {
		ofilename := fmt.Sprintf("mr-%v-%v", reply.TaskID, i)
		ofile, err := os.Create(ofilename)
		if err != nil {
			log.Fatalf("Map: cannot create output file %v", ofilename)
			break
		}
		m[i] = json.NewEncoder(ofile)
	}

	for _, kv := range kva {
		reduceID := ihash(string(kv.Key)) % reply.Nreduce
		if err = m[reduceID].Encode(&kv); err != nil {
			log.Fatalf("Map: cannot encode json %v", reply.Filename)
		}
	}

	if debug {
		fmt.Printf("Map: finished %v\n", reply.Filename)
	}
	call("Coordinator.SetMapDone", &TaskDoneArgs{IsMap: true, TaskID: reply.TaskID}, &TaskDoneReply{})
}

func doReduce(reducef func(string, []string) string, reply *RequestTaskReply) {
	// deserialize the JSON input file

	// read the intermediate key/value pairs from temporaray files, decode them, then concat together
	// only read from files that belong to us (use reply.TaskID to get our reduce task ID)
	intermediate := []KeyValue{}
	for i := 0; i < reply.Nfiles; i++ {
		ifilename := fmt.Sprintf("mr-%d-%d", i, reply.TaskID)
		if debug {
			fmt.Printf("reduce's infile %s\n", ifilename)
		}
		ifile, err := os.Open(ifilename)
		if err != nil {
			continue
		}
		dec := json.NewDecoder(ifile)
		for { // loop through each json object in the file and decode
			var decoded KeyValue
			if err = dec.Decode(&decoded); err != nil {
				break
			}
			intermediate = append(intermediate, decoded)
		}
	}

	sort.Sort(ByKey(intermediate))
	ofile, err := os.Create(reply.Filename)
	if err != nil {
		log.Fatalf("Reduce: cannot create output file %v", reply.Filename)
		return
	}

	// sort by keys first to group values by key. Then call reducef on each group of values.
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	if debug {
		fmt.Printf("Reduce: finished %v\n", reply.Filename)
	}
	call("Coordinator.SetReduceDone", &TaskDoneArgs{IsMap: false, TaskID: reply.TaskID}, &TaskDoneReply{})
}

// main/mrworker.go calls this function.
// keep asking for task in intervals then handle tasks
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		reply := RequestTaskReply{}
		args := RequestTaskArgs{}
		if !call("Coordinator.RequestTask", &args, &reply) {
			time.Sleep(300 * time.Millisecond)
			continue
		}

		if reply.Done { // everything is done. Terminate now
			return
		}

		if reply.IsMap { // assign Map task
			doMap(mapf, &reply)
		} else { // assign Reduce task
			doReduce(reducef, &reply)
		}
	}
}

/* // example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
} */

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	if debug {
		fmt.Println(err)
	}
	return false
}
