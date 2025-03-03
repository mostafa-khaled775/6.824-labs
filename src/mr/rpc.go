package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type PollTaskArgs struct {
	Id int
}

type PollTaskReply struct {
	Task MRTask
}

type HandleTaskResultsArgs struct {
	Task    MRTask
	Results []string
}

type HandleTaskResultsReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
