package mr

type ReqTaskArgs struct {
}

type ReqTaskReply struct {
	Task   Task
	Period Period
}

type TaskRespArgs struct {
	Index          int
	TaskSuccess    bool
	MiddleFilePath []string
}

type TaskRespReply struct {
	Period Period
}
