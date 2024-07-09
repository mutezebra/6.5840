package mr

import "time"

type Period int8

const (
	MapPeriod Period = iota
	ReducePeriod
	CompletePeriod
)

type TaskStatus int8

const (
	ReadyTaskStatus TaskStatus = iota
	RunningTaskStatus
	CompleteTaskStatus
)

const (
	HeartInterval = 500 * time.Millisecond
	TaskDuration  = 10 * HeartInterval
)
