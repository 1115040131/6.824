package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type MetaStatus int

const (
	Idle       MetaStatus = iota // 空闲
	InProgress                   // 运行中
	Completed                    // 完成
)

type State int

const (
	Map State = iota
	Reduce
	Exit
	Wait
)

type Task struct {
	Input        string // 要map的文件
	TaskState    State
	NReduce      int
	Id           int
	Intermediate []string // map产生的中间文件
	Output       string   // reduce后的文件
}

type Coordinator struct {
	// Your definitions here.
	TaskQueue        chan *Task        // 等待执行的task
	TaskMetas        map[int]*TaskMeta // task的元数据
	CoordinatorState State             // coordinator的阶段
	NReduce          int
	InputFiles       []string
	Intermediate     [][]string // Map任务产生的R个中间文件的信息
}

type TaskMeta struct {
	TaskStatus MetaStatus
	StartTime  time.Time
	TaskPtr    *Task
}

var mu sync.Mutex

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

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
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
	mu.Lock()
	defer mu.Unlock()
	ret := c.CoordinatorState == Exit

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskQueue:        make(chan *Task, max(nReduce, len(files))),
		TaskMetas:        make(map[int]*TaskMeta),
		CoordinatorState: Map,
		NReduce:          nReduce,
		InputFiles:       files,
		Intermediate:     make([][]string, nReduce),
	}

	// Your code here.
	// 切成16-64MB文件
	// 创建map任务
	c.createMapTask()

	// 启动coordinator服务器
	c.server()
	// 启动一个goroutine检查超时的任务
	go c.catchTimeOut()
	return &c
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func (c *Coordinator) catchTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		mu.Lock()
		if c.CoordinatorState == Exit {
			mu.Unlock()
			return
		}
		for _, task := range c.TaskMetas {
			if task.TaskStatus == InProgress && time.Now().Sub(task.StartTime) > 10*time.Second {
				task.TaskStatus = Idle
				c.TaskQueue <- task.TaskPtr
			}
		}
		mu.Unlock()
	}
}

func (c *Coordinator) createMapTask() {
	for idx, filename := range c.InputFiles {
		task := Task{
			Input:     filename,
			TaskState: Map,
			NReduce:   c.NReduce,
			Id:        idx,
		}
		c.TaskQueue <- &task
		c.TaskMetas[idx] = &TaskMeta{
			TaskStatus: Idle,
			TaskPtr:    &task,
		}
	}
}

func (c *Coordinator) createReduceTask() {
	c.TaskMetas = make(map[int]*TaskMeta)
	for idx, files := range c.Intermediate {
		task := Task{
			TaskState:    Reduce,
			NReduce:      c.NReduce,
			Id:           idx,
			Intermediate: files,
		}
		c.TaskQueue <- &task
		c.TaskMetas[idx] = &TaskMeta{
			TaskStatus: Idle,
			TaskPtr:    &task,
		}
	}
}

// coordinator等待worker调用
func (c *Coordinator) AssignTask(args *ExampleArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	if c.CoordinatorState == Exit {
		*reply = Task{TaskState: Exit}
	} else if len(c.TaskQueue) > 0 {
		*reply = *<-c.TaskQueue
		c.TaskMetas[reply.Id].TaskStatus = InProgress
		c.TaskMetas[reply.Id].StartTime = time.Now()
	} else {
		*reply = Task{TaskState: Wait}
	}
	return nil
}

func (c *Coordinator) TaskComplete(task *Task, reply *ExampleReply) error {
	mu.Lock()
	defer mu.Unlock()
	if task.TaskState != c.CoordinatorState || c.TaskMetas[task.Id].TaskStatus == Completed {
		return nil
	}

	c.TaskMetas[task.Id].TaskStatus = Completed
	switch task.TaskState {
	case Map:
		// 收集中间文件信息
		for reduceId, file := range task.Intermediate {
			c.Intermediate[reduceId] = append(c.Intermediate[reduceId], file)
		}
		if c.allTaskComplete() {
			// 进入Reduce阶段
			c.createReduceTask()
			c.CoordinatorState = Reduce
		}
	case Reduce:
		if c.allTaskComplete() {
			c.CoordinatorState = Exit
		}
	}

	return nil
}

func (c *Coordinator) allTaskComplete() bool {
	for _, task := range c.TaskMetas {
		if task.TaskStatus != Completed {
			return false
		}
	}
	return true
}
