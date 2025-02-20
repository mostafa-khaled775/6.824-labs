package mr

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"io/fs"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type MRWorker struct {
	mapf       func(string, string) []KeyValue
	reducef    func(string, []string) string
	taskNumber int
	done       chan struct{}
}

type IFilePartition struct {
	Partition []KeyValue
}

func (mrw *MRWorker) id() int {
	return os.Getpid()
}

func (mrw *MRWorker) tmpDir() string {
	return fmt.Sprintf("mrw-tmp-%d", mrw.id())
}

func (mrw *MRWorker) createTmpDir() error {
	return os.Mkdir(mrw.tmpDir(), fs.ModeDir|fs.ModePerm)
}

func (mrw *MRWorker) cleanTmpDir() error {
	return os.RemoveAll(mrw.tmpDir())
}

func MRWorkerNew(mapf func(string, string) []KeyValue, reducef func(string, []string) string) *MRWorker {
	return &MRWorker{
		mapf:       mapf,
		reducef:    reducef,
		taskNumber: 0,
		done:       make(chan struct{}),
	}
}

func (mrw *MRWorker) run() {
	task := mrw.pollTask()
	if task.Done {
		close(mrw.done)
		return
	}
	mrw.execute(task)
	mrw.taskNumber += 1
	mrw.run()
}

func (mrw *MRWorker) execute(task MRTask) {
	if task.MapTask != nil {
		mrw.submitTaskResults(task, mrw.executeMap(*task.MapTask))
	} else if task.ReduceTask != nil {
		mrw.submitTaskResults(task, mrw.executeReduce(*task.ReduceTask))
	} else {
		time.Sleep(10 * time.Millisecond)
	}
}

func (mrw *MRWorker) executeMap(t MRMapTask) []string {
	kvs := mrw.mapf(t.Key(), t.Value())
	kvMap := make([]IFilePartition, t.NReduce)
	for _, kv := range kvs {
		i := ihash(kv.Key) % t.NReduce
		kvMap[i].Partition = append(kvMap[i].Partition, kv)
	}

	intermediateFiles := make([]string, t.NReduce)
	for i, partition := range kvMap {
		file, err := os.CreateTemp("", fmt.Sprintf("mr-intermediate-%04d-%04d", i, t.NReduce))
		intermediateFiles[i] = file.Name()
		if err != nil {
			log.Fatalf("cannot create intermediate file %v", err)
		}
		defer file.Close()
		encoder := gob.NewEncoder(file)
		if err := encoder.Encode(partition); err != nil {
			log.Fatalf("cannot encode intermediate values %v", err)
		}
	}
	return intermediateFiles
}

func (mrw *MRWorker) executeReduce(t MRReduceTask) []string {
	// collect all key-values from intermediate files
	kvs := make([]KeyValue, 0)
	for _, ifile := range t.IntermediateFiles {
		file, err := os.Open(ifile)
		if err != nil {
			log.Fatalf("cannot open intermediate file %v", err)
		}
		defer file.Close()
		decoder := gob.NewDecoder(file)
		partition := IFilePartition{}
		if err := decoder.Decode(&partition); err != nil {
			log.Fatalf("cannot decode intermediate values %v", err)
		}
		kvs = append(kvs, partition.Partition...)
	}
	sort.Sort(ByKey(kvs))
	tf, err := os.Create(filepath.Join(mrw.tmpDir(), fmt.Sprintf("mr-reduce-%d", mrw.taskNumber)))
	if err != nil {
		log.Fatalf("cannot create temporary file %v", err)
	}
	buf := bufio.NewWriter(tf)

	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := mrw.reducef(kvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(buf, "%v %v\n", kvs[i].Key, output)

		i = j
	}

	if err := buf.Flush(); err != nil {
		log.Fatalln(err)
	}

	if err := os.Rename(tf.Name(), t.OutputFile); err != nil {
		log.Fatalf("cannot rename temporary file %v", err)
	}
	return []string{t.OutputFile}
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

	mrw := MRWorkerNew(mapf, reducef)
	if err := mrw.createTmpDir(); err != nil {
		log.Fatalf("error creating workers tmp directory: %v", err)
	}
	defer mrw.cleanTmpDir()
	go mrw.run()
	<-mrw.done
}

func (mrw *MRWorker) pollTask() MRTask {
	args := PollTaskArgs{os.Getpid()}
	reply := PollTaskReply{}
	err := mrw.call("Coordinator.PollTask", &args, &reply)
	if err != nil {
		log.Fatal(err)
	}
	return reply.Task
}

func (mrw *MRWorker) submitTaskResults(task MRTask, results []string) {
	args := HandleTaskResultsArgs{task, results}
	reply := HandleTaskResultsReply{}
	err := mrw.call("Coordinator.HandleTaskResults", &args, &reply)
	if err != nil {
		log.Fatal(err)
	}
}

// send an RPC request to the coordinator, wait for the response.
func (mrw *MRWorker) call(rpcname string, args interface{}, reply interface{}) error {

	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	call := c.Go(rpcname, args, reply, nil)
	select {
	case <-call.Done:
		return call.Error
	case <-time.After(time.Second * 1):
		close(mrw.done)
		return nil
	}
}
