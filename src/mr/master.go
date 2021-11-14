package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "strconv"
import "fmt"
import "time"
import "sync"


type Master struct {
	// Your definitions here.
	nMap int
	nReduce int

	mTasks []string
	rTasks []string

	mMapping map[string]int // filename -> taskNumber
	rMapping map[string]int

	mStates map[int]bool
	rStates map[int]bool // taks number -> task finish? true : false

	phase string // map / reduce

	isDone bool

	mu sync.mutex
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Assign(args *Args_Request, reply *Reply_Request) error {
	if PhaseEndsValidate(m) {
		fmt.Printf("All Phase Ends...Calling Done.")
		return nil
	}

	if m.phase == "map" {
		reply.TaskType = "map"
		reply.Task = m.mTasks[0] // always pick first one
		reply.TaskNumber = m.mMapping[reply.Task]
		reply.GeneralNum = m.nReduce

		showTasks(m, "map")
		showStates(m, "map")
		// call monitor in new thread
		go monitorTask(m, "map", reply.Task, reply.TaskNumber)

	} else if m.phase == "reduce" {
		fmt.Printf("reduce")

		reply.TaskType = "reduce"
		reply.Task = m.rTasks[0] // always pick first one
		reply.TaskNumber = m.rMapping[reply.Task]
		reply.GeneralNum = len(m.mStates)
		showTasks(m, "reduce")
		showStates(m, "reduce")
		showMapping(m, "reduce")

		go monitorTask(m, "reduce", reply.Task, reply.TaskNumber)

	} else {
		fmt.Errorf("Wrong phase: %v", m.phase)
	}
	

	return nil
}

func PhaseEndsValidate(m *Master) bool {
	if m.phase == "map" {
		if len(m.mTasks) == 0 {
			m.phase = "reduce"
		}
		return false
	} else {
		if len(m.rTasks) == 0 {
			m.isDone = true
			return true
		}
		return false
	}

}

// when worker finished a task, ReportDone is called 
func (m *Master) ReportDone(args *Args_Report, reply *Reply_Report) error {
	if args.TaskType == "map" {
		num := m.mMapping[args.Task]
		fmt.Printf("\nReportDone: CheckDone for map task %v", num)

		if num != args.TaskNumber {
			panic("This shit is not correct man!")
		}
		m.mStates[num] = true
	} else if args.TaskType == "reduce" {
		num := m.rMapping[args.Task]
		fmt.Printf("\nReportDone: CheckDone for reduce task %v", num)

		if num != args.TaskNumber {
			panic("This shit is not correct man!")
		}
		m.rStates[num] = true
	} else {
		fmt.Printf("No such task type: %s!", args.TaskType)
		panic("!!!")
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := m.isDone

	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// initialization 
	fmt.Printf("Starting initialization...")
	m.nMap = len(files)
	m.nReduce = nReduce

	m.mTasks = initialMapTask(files)
	m.rTasks = initialReduceTask(nReduce)

	m.mMapping = initialMapping(files, "map", nReduce)
	m.rMapping = initialMapping(files, "reduce", nReduce)

	// m.rMapping = initial
	// rMapping will be initialized in reduce phase

	m.mStates = initialMapStates(files)
	m.rStates = initialReduceStates(nReduce)

	m.phase = "map"
	m.isDone = false

	fmt.Printf("Starting server...")

	m.server()
	return &m
}

func initialMapping(files []string, taskType string, n int) map[string]int {
	if taskType == "map" {
		mapping := make(map[string]int)

		for i, file := range files {
			mapping[file] = i
		}

		return mapping
	} else {
		mapping := make(map[string]int)

		for i := 0; i < n; i++ {
			mapping[strconv.Itoa(i)] = i
		}
		return mapping
	}
	
}
func initialMapStates(files []string) map[int]bool {
	states := make(map[int]bool)

	for i, _ := range files {
		states[i] = false
	}

	return states
}

func initialReduceStates(nReduce int) map[int]bool {
	states := make(map[int]bool)
	i := 0

	for i < nReduce {
		states[i] = false
		i += 1
	}

	return states
}

func initialMapTask(files []string) []string {
	mtasks := make([]string, 0)
	for _, v := range files {
		mtasks = append(mtasks, v)
	}
	return mtasks
}

func monitorTask(m *Master, taskType string, task string, taskNumber int) {
	// sleep for time interval
	time.Sleep(10 * time.Second)

	var res bool
	// check if the task is finished
	if taskType == "map" {
		res = m.mStates[taskNumber]
	} else {
		res = m.rStates[taskNumber]
	}

	// if finished, clear it from tasks
	// if not finished, skip this bc it is still at the top

	if res {
		index := 0
		if taskType == "map" {
			m.mTasks = append(m.mTasks[:index], m.mTasks[index + 1:]...)
		} else {
			m.rTasks = append(m.rTasks[:index], m.rTasks[index + 1:]...)
		}
		fmt.Printf("[monitorTask] Task: %s is finished!", task)
		return
	}
	fmt.Printf("[monitorTask] Task: %s not finished!", task)
}

// func clearTask() []string {
// 	ss=append(ss[:index],ss[index+1:]...)
// }
func showStates(m *Master, taskType string) {
	fmt.Printf("\n %s showStates:\n", taskType)
	if taskType == "map" {
		for k, v := range m.mStates {
			fmt.Printf("%v -> %v\n", k, v)
		}
	} else {
		for k, v := range m.rStates {
			fmt.Printf("%v -> %v\n", k, v)
		}
	}
}
func showTasks(m *Master, taskType string) {
	fmt.Printf("\n %s showTasks:\n", taskType)
	if taskType == "map" {
		for k, v := range m.mTasks {
			fmt.Printf("%v -> %v\n", k, v)
		}
	} else {
		for k, v := range m.rTasks {
			fmt.Printf("%v -> %v\n", k, v)
		}
	}
	
}

func showMapping(m *Master, taskType string) {
	fmt.Printf("\n %s showMapping:\n", taskType)
	if taskType == "map" {
		for k, v := range m.mMapping {
			fmt.Printf("%v -> %v\n", k, v)
		}
	} else {
		for k, v := range m.rMapping {
			fmt.Printf("%v -> %v\n", k, v)
		}
	}
}

func initialReduceTask(nReduce int) []string {
	ret := make([]string, 0)
	i := nReduce - 1

	for i >= 0 {
		ret = append(ret, strconv.Itoa(i))
		i -= 1
	}

	return ret
}

