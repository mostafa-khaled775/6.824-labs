package mr

import (
	"fmt"
	"log"
	"os"
)

type MRTask struct {
	MapTask    *MRMapTask
	ReduceTask *MRReduceTask
	Done       bool
}

type MRTaskKind int

const (
	MapTaskKind    MRTaskKind = iota
	ReduceTaskKind MRTaskKind = iota
)

type MRTaskId struct {
	kind MRTaskKind
	id   int
}

func (t *MRTask) Id() MRTaskId {
	if t.MapTask != nil {
		return MRTaskId{MapTaskKind, t.MapTask.Id}
	}
	if t.ReduceTask != nil {
		return MRTaskId{ReduceTaskKind, t.ReduceTask.Id}
	}
	return MRTaskId{-1, -1}
}

func (task *MRTask) String() string {
	if task.MapTask != nil {
		return task.MapTask.String()
	}
	if task.ReduceTask != nil {
		return task.ReduceTask.String()
	}
	return "done"
}

type MRReduceTask struct {
	Id                int
	IntermediateFiles []string
	OutputFile        string
}

func (reducet *MRReduceTask) String() string {
	return fmt.Sprintf("reduce-%d", reducet.Id)
}

func makeReduceTask(Id int, IntermediateFiles []string, OutputFile string) MRTask {
	return MRTask{
		ReduceTask: &MRReduceTask{Id, IntermediateFiles, OutputFile},
	}
}

type MRMapTask struct {
	InputFile string
	NReduce   int
	Id        int
}

func (mapt *MRMapTask) String() string {
	return fmt.Sprintf("map-%d", mapt.Id)
}

func makeMapTask(InputFile string, NReduce int, Id int) MRTask {
	return MRTask{
		MapTask: &MRMapTask{InputFile, NReduce, Id},
	}
}

func (mapt *MRMapTask) Key() string {
	return mapt.InputFile
}

func (mapt *MRMapTask) Value() string {
	data, err := os.ReadFile(mapt.Key())
	if err != nil {
		log.Fatalf("Failed to read file: %s\n", mapt.Key())
	}
	return string(data)
}
