package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
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
	// CallExample()
	for {
		task := getTask()
		switch task.TaskState {
		case Map:
			mapper(&task, mapf)
		case Reduce:
			reducer(&task, reducef)
		case Wait:
			time.Sleep(time.Second)
		case Exit:
			return
		default:
			panic(fmt.Sprintf("unexpectde task state %v", task.TaskState))
		}
	}
}

func mapper(task *Task, mapf func(string, string) []KeyValue) {
	filename := task.Input
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	// map得到kv对, hash后缓存在intermediate中
	kva := mapf(filename, string(content))
	intermediate := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		slot := ihash(kv.Key) % task.NReduce
		intermediate[slot] = append(intermediate[slot], kv)
	}

	// 将缓存中的数据写入磁盘
	output := make([]string, 0)
	for i := 0; i < task.NReduce; i++ {
		dir, _ := os.Getwd()
		tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
		if err != nil {
			log.Fatal("Failed to create temp file", err)
		}
		enc := json.NewEncoder(tempFile)
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("Failed to write kv pair", err)
			}
		}
		tempFile.Close()
		oname := fmt.Sprintf("mr-%v-%v", task.Id, i)
		os.Rename(tempFile.Name(), oname)

		output = append(output, oname)
	}
	task.Intermediate = output
	taskComplete(task)
}

func reducer(task *Task, reducef func(string, []string) string) {
	// 从磁盘读取每个reduce的kv
	var intermediate []KeyValue
	for _, filename := range task.Intermediate {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		var kva []KeyValue
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
		intermediate = append(intermediate, kva...)
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%v", task.Id)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		//将相同的key放在一起分组合并
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		//交给reducef，拿到结果
		output := reducef(intermediate[i].Key, values)
		//写到对应的output文件
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	task.Output = oname
	taskComplete(task)
}

func getTask() Task {
	// 通过rpc从coordinator获取任务
	args := ExampleArgs{}
	reply := Task{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if !ok {
		fmt.Printf("call Coordinator.AssignTask failed!\n")
	}
	return reply
}

func taskComplete(task *Task) {
	// 通过rpc通知coordinator任务完成
	reply := ExampleReply{}
	ok := call("Coordinator.TaskComplete", task, &reply)
	if !ok {
		fmt.Printf("call Coordinator.TaskComplete failed!\n")
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
