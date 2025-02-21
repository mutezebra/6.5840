package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// PublishMapWorkArgs 向 coordinator 获取任务
type PublishMapWorkArgs struct {
}

type PublishMapWorkReply struct {
	Filepath string
	Index    int
	Period   int64
	NReduce  int
}

// CompleteMapWorkArgs 告知 coordinator 完成任务
type CompleteMapWorkArgs struct {
	Index     int
	Filepaths []string
}

type CompleteMapWorkReply struct {
	Period int64
}

// PublishReduceWorkArgs 向 coordinator 获取任务
type PublishReduceWorkArgs struct {
}

type PublishReduceWorkReply struct {
	Filepaths      []string
	ReduceSequence int
	Period         int64
}

// CompleteWorkArgs 告知 coordinator 完成任务
type CompleteReduceWorkArgs struct {
	ReduceSequence int
}

type CompleteReduceWorkReply struct {
	Period int64
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
