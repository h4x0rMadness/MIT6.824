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

	mIndex int
	rIndex int

	mMapping map[string]int // filename -> taskNumber
	rMapping map[string]int

	mStates map[int]bool
	rStates map[int]bool // taks number -> task finish? true : false

	phase string // map / reduce

	isDone bool


}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Assign(args *Args_Request, reply *Reply_Request) error {
	var mu sync.Mutex
	

	res := m.PhaseEndsValidate()

	if res == "wait" || res == "end" {
		reply.TaskType = res
		return nil
	} 

	if m.phase == "map" {
		mu.Lock()

		// fmt.Printf("\n[Master]mIndex before: %v", m.mIndex)

		m.mIndex += 1
		// fmt.Printf("\n[Master]mIndex after: %v", m.mIndex)
		reply.TaskType = "map"
		reply.Task = m.mTasks[m.mIndex - 1] // pick the one mIndex points to
		reply.TaskNumber = m.mMapping[reply.Task]
		reply.GeneralNum = m.nReduce

		// m.showTasks("map")
		// m.showStates("map")
		// m.showMapping("map")
		// fmt.Printf("mIndex: %v", m.mIndex)
		// call monitor in new thread
		// fmt.Printf("\n  [Master]Assigning for task %s, taskNumber: %v", reply.Task, reply.TaskNumber)

		
		
		mu.Unlock()

		go m.monitorTask("map", reply.Task, reply.TaskNumber)
		// m.mu.Unlock()
		// fmt.Println

	} else if m.phase == "reduce" {
		// fmt.Printf("reduce")
		mu.Lock()

		m.rIndex += 1

		reply.TaskType = "reduce"
		reply.Task = m.rTasks[m.rIndex - 1] // pick the one rIndex points to
		reply.TaskNumber = m.rMapping[reply.Task]
		reply.GeneralNum = len(m.mStates)

		// m.showStates("reduce")
		// 

		// m.mu.Lock()
		
		mu.Unlock()

		go m.monitorTask("reduce", reply.Task, reply.TaskNumber)
		// m.mu.Unlock()

	} else if m.phase == "done" {
		mu.Lock()
		m.isDone = true
		// fmt.Println("`n!!!!!!!!Master set to done")
		mu.Unlock()
		
	}
	
	// m.mu.Unlock()

	return nil
}

func (m *Master) PhaseEndsValidate() string{
	var mu sync.Mutex
	mu.Lock()
	defer mu.Unlock()
	if m.phase == "map" {
		// if not pointing to the end, skip

		if len(m.mTasks) > m.mIndex {
			return ""
		}
		// if at the end, check all states
		for _, v := range m.mStates {

			// if not finished, but there is nothing to assign
			// indicate this worker to skip and wait
			if !v {
				return "wait"
			}
		}

		// m.mu.Lock()
		m.phase = "reduce"
		// m.mu.Unlock()
		return ""  


	} else if m.phase == "reduce"{
		
		if len(m.rTasks) > m.rIndex {
			return ""
		}
		for _, v := range m.rStates {
			if !v {
				return "wait"
			}
		}
		// m.mu.Lock()
		m.phase = "done"
		// m.mu.Unlock()
		return "end"
	}
	// mu.Unlock()
	return ""
}

// when worker finished a task, ReportDone is called 
func (m *Master) ReportDone(args *Args_Report, reply *Reply_Report) error {
	var mu sync.Mutex
	mu.Lock()
	

	if args.TaskType == "map" {
		num := m.mMapping[args.Task]
		// fmt.Printf("\nReportDone: CheckDone for map task %v", num)

		if num != args.TaskNumber {
			panic("This shit is not correct man!")
		}
		// m.mu.Lock()
		m.mStates[num] = true
		// m.mu.Unlock()
		
	} else if args.TaskType == "reduce" {
		num := m.rMapping[args.Task]
		// fmt.Printf("\nReportDone: CheckDone for reduce task %v", num)

		if num != args.TaskNumber {
			panic("This shit is not correct man!")
		}
		// m.mu.Lock()
		m.rStates[num] = true
		// m.mu.Unlock()
		
	} else {
		// fmt.Printf("No such task type: %s!", args.TaskType)
		panic("\n!!!\n")
	}
	mu.Unlock()

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
	var mu sync.Mutex
	mu.Lock()
	defer mu.Unlock()

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
	// fmt.Printf("Starting initialization...")
	m.nMap = len(files)
	m.nReduce = nReduce

	m.mTasks = initialMapTask(files)
	m.rTasks = initialReduceTask(nReduce)

	m.mMapping = initialMapping(files, "map", nReduce)
	m.rMapping = initialMapping(files, "reduce", nReduce)

	m.mIndex = 0
	m.rIndex = 0

	m.mStates = initialMapStates(files)
	m.rStates = initialReduceStates(nReduce)

	m.phase = "map"
	m.isDone = false


	// fmt.Printf("Starting server...")

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

func (m *Master) monitorTask(taskType string, task string, taskNumber int) {
	var mu sync.Mutex
	// mu.Lock()
	// sleep for time interval
	time.Sleep(10 * time.Second)

	var res bool
	// check if the task is finished
	if taskType == "map" {
		mu.Lock()
		res = m.mStates[taskNumber]
		mu.Unlock()

	} else {
		mu.Lock()
		res = m.rStates[taskNumber]
		mu.Unlock()

	}

	if res {
		// fmt.Printf("\n[monitorTask] Task: %s is finished!", task)
	} else {
		// fmt.Printf("\n[monitorTask] Task: %s not finished! Adding it to the end of task list.", task)

		if taskType == "map" {
			mu.Lock()
			m.mTasks = append(m.mTasks, task)
			// m.mIndex += 1
			mu.Unlock()
		} else if taskType == "reduce"{
			mu.Lock()
			m.rTasks = append(m.rTasks, task)
			// m.rIndex += 1
			mu.Unlock()
		}
	}
	// mu.Unlock()
	
}

// func clearTask() []string {
// 	ss=append(ss[:index],ss[index+1:]...)
// }
func (m *Master) showStates(taskType string) {
	// fmt.Printf("\n %s showStates:\n", taskType)
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
func (m *Master) showTasks(taskType string) {
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

func (m *Master) showMapping(taskType string) {
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
	

	for i := 0; i < nReduce; i++ {
		ret = append(ret, strconv.Itoa(i))
	}

	return ret
}

