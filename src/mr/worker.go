package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "errors"
import "encoding/json"
import "sort"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// 
// for sorting by key.
//
type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	reply := RequestTaskReply{}
	args := RequestTaskArgs{1}

	fmt.Println("in worker\n")
	err := CallRequestTask(&args, &reply)
	if err != nil {
		log.Fatalf("Failed to request task from coordinator %v", reply.Filename)
		return
	}

	if reply.IsMap {		// Map worker
		file, err := os.Open(reply.Filename)
		if err != nil {
			log.Fatalf("Map: cannot open %v", reply.Filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("Map: cannot read %v", reply.Filename)
		}
		file.Close()
		kva := mapf(reply.Filename, string(content))
		

		// write the intermediate key/value pairs into a temporaray file
		// right now, use a static filename
		ofile, err := os.Create("mr-0-0")
		if err != nil {
			log.Fatalf("Reduce: cannot create %v", reply.Filename)
		}
		
		enc := json.NewEncoder(ofile)
		if err = enc.Encode(&kva); err != nil {
			log.Fatalf("Map: cannot encode json %v", reply.Filename)
		}

	} else {	// Reduce worker
	
		// deserialize the JSON input file 
		// hardcode the input filename for now
		ifile, err := os.Open("mr-0-0");
		if err != nil {
			log.Fatalf("Reduce: cannot open input file %v", reply.Filename);
		}
		intermediate := []KeyValue{}		
		dec := json.NewDecoder(ifile)
		dec.Decode(&intermediate)

		sort.Sort(ByKey(intermediate))

		ofile, err := os.Create(reply.Filename)
		if err != nil {
			log.Fatalf("Reduce: cannot create %v", reply.Filename)
		}

		// sort by keys first to group values by key. Then call reducef on each group of values.
		for i := 0; i < len(intermediate); {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value);
			}
			output := reducef(intermediate[i].Key, values)
			
			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}
	}
}

// Request a task from the coordinator
func CallRequestTask (args *RequestTaskArgs, reply *RequestTaskReply) error {

	ok := call("Coordinator.RequestTask", args, reply)
	if ok {
		fmt.Printf("reply.Filename: %v\n", reply.Filename)
		return nil
	} else {
		return errors.New("call failed\n")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	fmt.Println(err)
	return false
}

