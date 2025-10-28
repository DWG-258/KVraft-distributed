package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		args := TaskArgs{}
		args.Type = WaitingType
		args.Finshed = false

		reply, err := calltoCoordinator(args)
		if err != nil {
			log.Fatal("cannot call to coordinator")
			return
		}

		if reply.Exit {
			break
		}

		Inputfilename := reply.Filename

		//Map task , open all input files and process them using mapf
		if reply.Type == MapfType {
			file, err := os.Open(Inputfilename)
			if err != nil {
				log.Fatal("cannot open map input file")
				return
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatal("cannot read map input file content")
				return
			}
			//call mapf
			kva := mapf(Inputfilename, string(content))

			for _, kv := range kva {
				//0-10
				reduceID := ihash(kv.Key) % 10

				ofile, err := os.OpenFile("mr-"+strconv.Itoa(reply.TaskId)+"-"+strconv.Itoa(reduceID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
				if err != nil {
					log.Fatal("cannot open map output file")
					return
				}

				enc := json.NewEncoder(ofile)
				err = enc.Encode(&kv)
				ofile.Close()
			}
			args.Type = MapfType
			args.Finshed = true
			//RPC call to coordinator with map output
			calltoCoordinator(args)
			file.Close()

		}

		//Reduce task
		if reply.Type == ReducefType {
			//reduceID
			reduceID := reply.TaskId
			pattern := "/home/ezio/mit6824/6.5840/src/main/mr-*-" + strconv.Itoa(reduceID)
			ifiles, err := filepath.Glob(pattern)
			if err != nil {
				log.Fatal("cannot open reduce input file")
				return
			}

			KeyValues := make(map[string][]string)

			for _, ifilename := range ifiles {
				print("reduce input file " + ifilename + "\n")
				matched, _ := regexp.MatchString(`^mr-\d-\d+$`, filepath.Base(ifilename))
				if !matched {
					print("cannot match reduce input file")
					break
				}
				ifile, err := os.Open(ifilename)

				if err != nil {
					log.Fatal("cannot open reduce input file")
					return
				}

				dec := json.NewDecoder(ifile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err == io.EOF {
						break
					} else if err != nil {

						log.Fatal("cannot decode input file")
						return
					}
					KeyValues[kv.Key] = append(KeyValues[kv.Key], kv.Value)
				}
				ifile.Close()

			}

			//output file
			ofile, err := os.OpenFile("mr-out-"+strconv.Itoa(reduceID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
			if err != nil {
				log.Fatal("cannot open reduce output file")
				return
			}

			for k, v := range KeyValues {
				output := reducef(k, v)
				fmt.Fprintf(ofile, "%v %v\n", k, output)
			}
			args.Type = ReducefType
			args.Finshed = true
			calltoCoordinator(args)
			ofile.Close()

		}

		time.Sleep(time.Second)

	}

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

func calltoCoordinator(args TaskArgs) (TaskReply, error) {

	reply := TaskReply{}

	// send the RPC request, wait for the reply.
	result := call("Coordinator.TaskCall", &args, &reply)

	if result {
		fmt.Printf("Task reply %v\n", reply)
		return reply, nil
	} else {
		fmt.Printf("call failed!\n")
		return reply, fmt.Errorf("call failed")
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
