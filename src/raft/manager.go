package raft

import (
	"sync"
	"time"
)

type TaskManager struct {
	mu                  sync.Mutex
	tasks               map[taskType][]task
	defaultTaskQueueNum int
	defaultTaskQueueKey int
	taskTypeIndex       int // type -> Number
}

func NewTaskManager(defaultTaskQueueNum int) *TaskManager {
	manager := &TaskManager{
		mu:                  sync.Mutex{},
		tasks:               make(map[taskType][]task),
		taskTypeIndex:       0,
		defaultTaskQueueNum: defaultTaskQueueNum,
		defaultTaskQueueKey: 0,
	}
	for i := 0; i < defaultTaskQueueNum; i++ {
		manager.NewTaskType()
	}
	return manager
}

type task struct {
	f func()
}

func (t *task) execute() {
	t.f()
}

type taskType int // start from 0

const (
	RandomQueue int = -1
)

func (m *TaskManager) NewTaskType() int {
	m.mu.Lock()
	tp := taskType(m.taskTypeIndex)
	m.tasks[tp] = make([]task, 0, 4)
	m.taskTypeIndex++
	m.mu.Unlock()
	go m.executeTask(tp)
	return int(tp)
}

func (m *TaskManager) AddTask(tp int, f ...func()) {
	tp2 := taskType(tp)
	m.mu.Lock()
	defer m.mu.Unlock()
	if tp == RandomQueue {
		tp2 = taskType(m.defaultTaskQueueKey % m.defaultTaskQueueNum)
		m.defaultTaskQueueKey++
	}
	if _, ok := m.tasks[tp2]; !ok {
		DPrintf("tp未注册,tp:%d", tp2)
		return
	}
	tasks := make([]task, len(f))
	for i := range tasks {
		tasks[i] = task{f: f[i]}
	}
	m.tasks[tp2] = append(m.tasks[tp2], tasks...)
}

func (m *TaskManager) ClearTaskQueue(tp int) {
	tp2 := taskType(tp)
	m.mu.Lock()
	m.tasks[tp2] = make([]task, 0)
	m.mu.Unlock()
}

func (m *TaskManager) executeTask(tp taskType) {
	fn := func() {
		for {
			time.Sleep(10 * time.Microsecond)
			m.mu.Lock()
			if len(m.tasks[tp]) == 0 {
				m.mu.Unlock()
				continue
			}
			tasks := make([]task, len(m.tasks[tp]))
			copy(tasks, m.tasks[tp])
			m.tasks[tp] = make([]task, 0, cap(m.tasks[tp]))
			m.mu.Unlock()
			for i := range tasks {
				tasks[i].execute()
			}
		}
	}
	go fn()
}
