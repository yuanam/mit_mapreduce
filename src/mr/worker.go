package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
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

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce  编号
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// status
func status_finished(taskid int) bool {
	args := RequestArgs{}
	args.Taskid = taskid
	args.WorkerState = 1
	reply := ResponseArgs{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		fmt.Printf("reply.Filename %v\n", reply.Filename)
	} else {
		fmt.Printf("call failed!\n")
	}
	return ok
}

/* simi mrsequential.go */

// todo _ wrong shuffle_{"Key":"c","Value":"138885"} out_none
// ways: defer close

// map + status_change func
func doMapTask(mapf func(string, string) []KeyValue, response ResponseArgs) {

	// intpu-map-keyvalue + partition() + sort/spill + merge + (disk)

	filename := response.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	sort.Sort(ByKey(kva))

	// save all_file_
	file_list := make([]*json.Encoder, response.Reducenumber) // todo
	// json study
	/*
	 *   enc := json.NewEncoder(file)
	 *   for _, kv := ... {
	 *   	err := enc.Encode(&kv)
	 * 	 }
	 */

	for i := 0; i < response.Reducenumber; i++ {
		oname := fmt.Sprintf("mr-shuffle-%d-%d", response.Taskid, i)
		ofile, _ := os.Create(oname)

		encoder := json.NewEncoder(ofile)
		file_list[i] = encoder

	}

	for _, kv := range kva {
		bucket_id := ihash(kv.Key) % response.Reducenumber
		err := file_list[bucket_id].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot map #{response.Taskid}")
		}
	}

	// result_name_ mr-shuffle-x-xx

	status_finished(response.Taskid)
}

// reduce + status_change func
func doReduceTask(reducef func(string, []string) string, response ResponseArgs) {

	// sort/spill + merge + reduce-output

	intermediate := []KeyValue{}
	/*
	  dec := json.NewDecoder(file)
	  for {
	    var kv KeyValue
	    if err := dec.Decode(&kv); err != nil {
	      break
	    }
	    kva = append(kva, kv)
	  }
	*/
	for i := 0; i < response.Mapnumber; i++ {
		filename := fmt.Sprintf("mr-shuffle-%d-%d", i, response.Taskid) // todo
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		defer file.Close()
		decoder := json.NewDecoder(file)
		count := 0
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {

				break
			}
			count += 1
			intermediate = append(intermediate, kv)
		}

	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", response.Taskid)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
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
	ofile.Close()

	status_finished(response.Taskid)
}

// request_
func doHeartBeat() ResponseArgs {
	// declare an argument structure.
	args := RequestArgs{}
	// fill in the argument(s).
	args.WorkerState = 0
	// declare a reply structure.
	reply := ResponseArgs{}
	// send the RPC request, wait for the reply.
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		fmt.Printf("reply.Filename %v\n", reply.Filename)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply

}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// wait for request
	for {
		response := doHeartBeat() // JobType
		log.Printf("Worker: receive coordinator's heartbeat %v \n", response)
		switch response.Jobtype {
		case Mapjob:
			doMapTask(mapf, response)
		case Reducejob:
			doReduceTask(reducef, response)
		case Waitjob:
			time.Sleep(1 * time.Second) // wait for coordinator's stop
		case Completejob:
			return
		default:
			panic(fmt.Sprintf("unexpected jobType %v", response.Jobtype))
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
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
}

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

	fmt.Println(err)
	return false
}
