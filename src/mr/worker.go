package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "strconv"
import "encoding/json"
import "sort"
import "time"



//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
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
	for true {
		// Your worker implementation here.
		fmt.Printf("Starting a worker...\n")

		// Require a task from master
		taskReply := RequestTask()

		if taskReply == (Reply_Request{}) {
			fmt.Printf("RequestTask gets nil!")
			return
		}

		// Read back intermediate file if needed

		// Read the file
		task := taskReply.Task
		taskType := taskReply.TaskType
		taskNum := taskReply.TaskNumber
		GeneralNum := taskReply.GeneralNum

		fmt.Printf("RequestTask, file: %s, type: %s , num: %v, GeneralNum: %v", 
			task, taskType, taskNum, GeneralNum)


		// Transfer it to corresponding Map/Reduce function
		if taskType == "map" {
			fmt.Printf("Map Task Handling...")

			file, err := os.Open(task)

			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", task)
			}
			file.Close()

			bucketX := strconv.Itoa(taskNum)

			kva := mapf(task, string(content))

			sort.Sort(ByKey(kva))
			
			// Create container for all keys
			container := make(map[int][]KeyValue)	

			for _, kv := range kva {
				cKey := ihash(kv.Key) % GeneralNum
				container[cKey] = append(container[cKey], kv)
			}

			for key, pairs := range container {
	    		curFileName := "mr-" + bucketX + "-" + strconv.Itoa(key) +".json"

	    		ofile, _ := os.Create(curFileName)

	    		lines := ""

	    		for _, kv := range pairs {
	    			str := "\n" + kv.Key + " " + kv.Value
					lines += str
	    		}

	    		ioutil.WriteFile(curFileName, []byte(lines), 0644)
	    		
	    		enc := json.NewEncoder(ofile)
				for _, kv := range pairs {
			    	enc.Encode(&kv)		
				}
				ofile.Close()
			}
		} else {
			fmt.Printf("Reduce Task Handling...")

			oname := "mr-out-" + strconv.Itoa(taskNum)
			ofile, _ := os.Create(oname)

			ii := 0
			for ii < GeneralNum {
				fmt.Printf("\n ii: %v", ii)
				// 
				kva := make([]KeyValue, 0)

				filename := "mr-" + strconv.Itoa(ii) + "-" + strconv.Itoa(taskNum) +".json"
			    // file, _ := ioutil.ReadFile(filename)
		    	file, _ := os.Open(filename)

		    	fmt.Printf("\nReduce Loop filename:%s", filename)
				// decode json file
				dec := json.NewDecoder(file)
			  	for {
			    	var kv KeyValue
			    	if err := dec.Decode(&kv); err != nil {
			      		break
			    	}
			    	kva = append(kva, kv)
			  	}
				// throw to reduce function
				i := 0
				for i < len(kva) {
					j := i + 1
					for j < len(kva) && kva[j].Key == kva[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, kva[k].Value)
					}
					output := reducef(kva[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

					i = j
				}
				// write result
				ii += 1

			}
			ofile.Close()
		}

		// Call back master to tell task finished & record file name
		Report(taskType, task, taskNum)

		fmt.Printf("Worker thread ends, sleeping...")
		time.Sleep(5 * time.Second)

	}
}

func RequestTask() Reply_Request {
	args := Args_Request{}

	// declare a reply structure
	reply := Reply_Request{}

	// send the RPC request, wait for the reply.
	call("Master.Assign", &args, &reply)


	// reply
	fmt.Printf("\n Calling RequestTask...")

	return reply
}

func Report(taskType string, task string, taskNumber int) Reply_Report {
	args := Args_Report{}

	reply := Reply_Report{}

	args.TaskType = taskType
	args.Task = task
	args.TaskNumber = taskNumber

	call("Master.ReportDone", &args, &reply)

	fmt.Printf("\nCalling Report for task: %s, type: %s", task, taskType)

	return reply
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
