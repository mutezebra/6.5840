package mr

import (
	"sync/atomic"
	"time"
)

type Task interface {
	Expire(expire time.Duration, now *time.Time) bool
	SetDone()
	Done() bool
	Reset()
}

type task struct {
	done  atomic.Bool
	start time.Time
}

func newTask() *task {
	t := &task{
		done:  atomic.Bool{},
		start: time.Now(),
	}
	return t
}

// Expire 判断这个任务是否超时
func (t *task) Expire(expire time.Duration, now *time.Time) bool {
	return t.start.Add(expire).Before(*now)
}

// SetDone 主要是为了代码更加语义化以及保证并发安全。避免一个任务被重新分配后但两个 consumer 都完成而导致的竞争
func (t *task) SetDone() {
	t.done.Store(true)
}

// Done 同上
func (t *task) Done() bool {
	return t.done.Load()
}

// Reset 任务被再分配时，重置开始时间
func (t *task) Reset() {
	t.start = time.Now()
}

type MapTask struct {
	filepath string
	index    int
	*task
}

type ReduceTask struct {
	filepath       []string
	reduceSequence int
	*task
}
