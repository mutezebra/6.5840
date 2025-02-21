package mr

func (c *Coordinator) PublishMapWork(args *PublishMapWorkArgs, reply *PublishMapWorkReply) error {
	if c.Done() || c.period != MapPeriod {
		reply.Period = c.period
		return nil
	}
	reply.Period = MapPeriod

	if len(c.taskCh) != 0 { // 如果还有任务,就尝试配分
		t, ok := <-c.taskCh
		if !ok || t.Done() { // 防止幻读(两个 goroutine 同时从 ch 中读，但 ch 只有一个数据)以及如果任务已经完成的情况
			return nil
		}
		t.Reset() // 重置过期时间
		ta := t.(*MapTask)
		reply.Filepath = ta.filepath
		reply.Index = ta.index
		reply.NReduce = c.nReduce
		c.suspendTask(ta) // 挂起任务
	}
	// 没有任务直接退出，等待新 period 或者任务的再分配

	return nil
}

func (c *Coordinator) CompleteMapWork(args *CompleteMapWorkArgs, reply *CompleteMapWorkReply) error {
	if c.Done() {
		reply.Period = c.period
		return nil
	}

	key := GetmapSuspendKey(args.Index)
	t, ok := c.suspendTasks.Load(key)
	if !ok { // 这种情况是过期了然后被再分配了，但是原来那个导致任务过期的 worker又提交了。那直接 return 就可以了
		return nil
	}
	ta := t.(*MapTask)
	ta.SetDone() // 如果在 set 的过程中或者断言的过程中被再消费了也没关系，只要设置为 done 了，publishRPC 和 c.reconsumeTask 会进行判断

	c.mu.Lock()
	defer c.mu.Unlock()
	for i, filepath := range args.Filepaths { // map 完成后返回的若干个中间文件，防止 data race 所以用锁保护
		if c.middleFiles[i] == nil {
			c.middleFiles[i] = make([]string, 0)
		}
		c.middleFiles[i] = append(c.middleFiles[i], filepath)
	}

	return nil
}

func (c *Coordinator) PublishReduceWork(args *PublishReduceWorkArgs, reply *PublishReduceWorkReply) error {
	if c.Done() || c.period != ReducePeriod {
		reply.Period = c.period
		return nil
	}
	reply.Period = c.period

	if len(c.taskCh) != 0 {
		t, ok := <-c.taskCh
		if !ok || t.Done() {
			return nil
		}
		t.Reset()
		ta := t.(*ReduceTask)
		reply.Filepaths = ta.filepath
		reply.ReduceSequence = ta.reduceSequence
		c.suspendTask(ta)
	}

	return nil
}

func (c *Coordinator) CompleteReduceWork(args *CompleteReduceWorkArgs, reply *CompleteReduceWorkReply) error {
	if c.Done() {
		reply.Period = c.period
		return nil
	}

	key := GetReduceSuspendKey(args.ReduceSequence)
	t, ok := c.suspendTasks.Load(key)
	if !ok {
		return nil
	}
	ta := t.(*ReduceTask)
	ta.SetDone()
	return nil
}
