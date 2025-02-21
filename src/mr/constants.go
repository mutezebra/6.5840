package mr

import (
	"fmt"
	"time"
)

const (
	middleFileFormat = "mr-%d-%d"  // mr - mapIndex - reduceIndex
	outputFileFormat = "mr-out-%d" // map - reduceIndex

	// 判断任务是否超时以及是否需要再消费的间隔
	reconsumeInterval = 10 * time.Millisecond
	// 任务过期时间, 根据任务的要求固定为 10秒
	taskExpireTime = 10 * time.Second
	// worker 休眠时间，一般是等待其他的 worker 做完手上的工作，然后进入下一时期
	workerSleepInterval = 1 * time.Second
)

const (
	publishMapRPC  = "Coordinator.PublishMapWork"
	completeMapRPC = "Coordinator.CompleteMapWork"

	publicReduceRPC   = "Coordinator.PublishReduceWork"
	completeReduceRPC = "Coordinator.CompleteReduceWork"
)

const (
	// 用于对任务标记唯一 key
	mapSuspendKeyFormat    = "map-%d"
	reduceSuspendKeyFormat = "reduce-%d"
)

func GetmapSuspendKey(index int) string {
	return fmt.Sprintf(mapSuspendKeyFormat, index)
}

func GetReduceSuspendKey(index int) string {
	return fmt.Sprintf(reduceSuspendKeyFormat, index)
}
