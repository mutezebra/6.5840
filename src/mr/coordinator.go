package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/samber/lo"
)

type Coordinator struct {
	files       []string
	middleFiles [][]string   // reduceIndex -> middleFiles
	mu          sync.RWMutex // 主要用于保护 files ，以及  middleFiles

	nReduce      int
	period       int64
	suspendTasks sync.Map     // 用于标记那些被分配了但是还没有完成的 task，同时提供了一个统一的入口来进行任务超时和再分配的判断
	suspendCount atomic.Int64 // 由于sync.Map 不具备查看容量的能力，所以引入这个变量
	taskCh       chan Task
}

const (
	ReadyPeriod int64 = -1 + iota
	MapPeriod
	ReducePeriod
	CompletePeriod
)

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// reconsumeTask 定时检查所有被挂起的任务的状态
// 如果已经完成了那就直接释放，否则重新丢入 channel 中等待分配
func (c *Coordinator) reconsumeTask() {
	c.suspendTasks.Range(func(key, value any) bool {
		t := value.(Task)
		now := time.Now()
		if t.Done() { // 如果这个 task 已经被标记完成，那就删除挂起的任务
			c.suspendTasks.Delete(key)
			c.suspendCount.Add(-1)
		} else if t.Expire(taskExpireTime, &now) { // 如果这个 task 过期了，那就认为当时分配到这个 task 的 worker 失联了，将会对这个任务再分配，同时取消挂起状态
			c.suspendTasks.Delete(key)
			c.suspendCount.Add(-1)
			c.taskCh <- t
		}
		return true
	})
}

// suspendTask 挂起一个任务。当一个任务进入执行状态时，这个任务会被 coordinator 挂起，然后定时检查状态
func (c *Coordinator) suspendTask(item Task) {
	switch t := item.(type) {
	case *MapTask:
		c.suspendTasks.Store(GetmapSuspendKey(t.index), t)
		c.suspendCount.Add(1)
	case *ReduceTask:
		c.suspendTasks.Store(GetReduceSuspendKey(t.reduceSequence), t)
		c.suspendCount.Add(1)
	default:
		return
	}
}

// TryIteration 尝试进行迭代
func (c *Coordinator) TryIteration() {
	if len(c.taskCh) != 0 || c.suspendCount.Load() != 0 {
		return
	}
	atomic.AddInt64(&c.period, +1)
	if c.period == MapPeriod {
		c.initMapTask()
	}
	if c.period == ReducePeriod {
		c.initReduceTask()
	}
}

// initMapTask 向 channel 中传递 map 的任务
func (c *Coordinator) initMapTask() {
	lo.ForEach(c.files, func(file string, index int) {
		c.taskCh <- &MapTask{
			filepath: file,
			index:    index,
			task:     newTask(),
		}
	})
}

func (c *Coordinator) initReduceTask() {
	lo.ForEach(c.middleFiles, func(items []string, index int) {
		c.taskCh <- &ReduceTask{
			filepath:       items,
			reduceSequence: index,
			task:           newTask(),
		}
	})
}

func (c *Coordinator) Done() bool {
	return c.period == CompletePeriod
}

func (c *Coordinator) start() {
	for {
		c.TryIteration()
		c.reconsumeTask()
		if c.Done() {
			c.stop()
			return
		}
		time.Sleep(reconsumeInterval)
	}
}

func (c *Coordinator) stop() {
	close(c.taskCh)
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	_ = rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	size := max(len(files), nReduce) // 这里是为了偷懒，直接让 size 为两者的最大值，这样初始化的时候不用考虑阻塞(开一个 goroutine 其实也可以，这里是为了减少复杂度)
	c := Coordinator{
		files:        files,
		middleFiles:  make([][]string, nReduce),
		nReduce:      nReduce,
		period:       ReadyPeriod,
		suspendTasks: sync.Map{},
		suspendCount: atomic.Int64{},
		taskCh:       make(chan Task, size),
		mu:           sync.RWMutex{},
	}

	go c.start()
	c.server()
	return &c
}
