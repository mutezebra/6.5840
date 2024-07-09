package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Task struct {
	Period    Period
	Files     []string
	Index     int
	ReduceNum int
}

type Coordinator struct {
	mapFiles    []string
	reduceFiles [][]string // mr-X-Y. Y -> reduceFileIndex
	mapNum      int
	reduceNum   int
	mu          sync.Mutex

	period     Period
	taskChan   chan Task
	taskTimer  map[int]int64 // index -> timestamp
	taskStatus []TaskStatus
}

func (c *Coordinator) server() {
	_ = rpc.Register(c)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatalf("failed when listen, error: %v\n", err)
	}
	go func() {
		err = http.Serve(l, nil)
		if err != nil {
			log.Fatalf("failed when start HTTP Server,error: %v\n", err)
		}
	}()
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapFiles:    files,
		reduceFiles: make([][]string, nReduce),
		mapNum:      len(files),
		reduceNum:   nReduce,
		mu:          sync.Mutex{},
		period:      0,
		taskChan:    make(chan Task, nReduce),
		taskTimer:   make(map[int]int64),
		taskStatus:  make([]TaskStatus, len(files)),
	}
	for i := range c.taskStatus {
		c.taskStatus[i] = ReadyTaskStatus
	}

	go c.scheduleTask()
	go c.checkHeart()

	c.server()
	return &c
}

func (c *Coordinator) Done() bool {
	return c.period == CompletePeriod
}

func (c *Coordinator) ReceiveTaskReq(args *ReqTaskArgs, reply *ReqTaskReply) error {
	reply.Period = c.period // 先定义period

	c.mu.Lock() // 避免数据竞争
	defer c.mu.Unlock()
	task, ok := <-c.taskChan
	if !ok { // 如果channel关闭了那就直接退出,同时前面的period已经是CompletePeriod了
		return nil
	}
	reply.Task = task                                // 设置Task
	c.taskTimer[task.Index] = time.Now().UnixMilli() // 设置Timer
	return nil
}

func (c *Coordinator) ReceiveTaskResp(args *TaskRespArgs, reply *TaskRespReply) error {
	reply.Period = c.period // 先定义period

	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskSuccess { // 如果任务成功了那就标记为完成
		c.taskStatus[args.Index] = CompleteTaskStatus
		delete(c.taskTimer, args.Index) // 删除Timer
	} else { // 否则挂起，等待scheduler分配
		c.taskStatus[args.Index] = ReadyTaskStatus
	}
	if c.period == MapPeriod {
		c.processMiddleFile(args.MiddleFilePath)
	}
	return nil
}

func (c *Coordinator) scheduleTask() {
	for {
		time.Sleep(HeartInterval)
		task := Task{
			Period:    c.period,
			Files:     nil,
			Index:     0,
			ReduceNum: c.reduceNum,
		}
		switch c.period {
		case MapPeriod: // 如果是map阶段
			for i, status := range c.taskStatus {
				if status == ReadyTaskStatus { // 并且任务状态是等待中
					task.Files = []string{c.mapFiles[i]} // 那就把这个任务挂起
					task.Index = i
					c.taskChan <- task
				}
			}
		case ReducePeriod:
			for i, status := range c.taskStatus {
				if status == ReadyTaskStatus {
					task.Files = c.reduceFiles[i]
					task.Index = i
					c.taskChan <- task
				}
			}
		default:
			return
		}
	}
}

func (c *Coordinator) checkHeart() {
	expect := TaskDuration.Milliseconds()
	overTime := func(cost int64) bool {
		return cost > expect
	}

	for {
		time.Sleep(HeartInterval)
		now := time.Now().UnixMilli()
		switch c.period {
		case MapPeriod, ReducePeriod:
			for i, status := range c.taskStatus { // 遍历所有任务
				if status == RunningTaskStatus { // 如果是执行中再进行判断
					cost := now - c.taskTimer[i] // 计算已经耗费的时间
					if overTime(cost) {          // 如果超出预期就将任务挂起
						c.taskStatus[i] = ReadyTaskStatus
					}
				}
			}
			break
		default: // 退出goroutine
			return
		}

		// checkHeart之后顺便检查当前状态
		finish := true
		for _, status := range c.taskStatus {
			if status != CompleteTaskStatus {
				finish = false
			}
		}
		if finish {
			if c.period == MapPeriod {
				c.period = ReducePeriod
				c.taskStatus = make([]TaskStatus, c.reduceNum)
				for i := range c.taskStatus {
					c.taskStatus[i] = ReadyTaskStatus
				}
			} else if c.period == ReducePeriod {
				c.period = CompletePeriod
			}
		}
	}
}

func (c *Coordinator) processMiddleFile(filepaths []string) {
	getIndex := func(path string) int {
		parts := strings.Split(path, "-")
		part := parts[2]
		i, _ := strconv.Atoi(part)
		return i
	}

	for _, filepath := range filepaths {
		if filepath == "" {
			continue
		}
		index := getIndex(filepath)
		c.reduceFiles[index] = append(c.reduceFiles[index], filepath)
	}
}
