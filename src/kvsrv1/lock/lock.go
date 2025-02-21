package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"time"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck      kvtest.IKVClerk
	state   string
	secret  string
	version rpc.Tversion
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	lk.state = l
	lk.secret = kvtest.RandValue(SecretBits)
	lk.version = rpc.ZERO
	return lk
}

func (lk *Lock) Acquire() {
	for {
		time.Sleep(WaitInterval)
		value, ver, e := lk.ck.Get(lk.state)
		if value == lk.secret {
			// 如果是自己的锁，那给 version 赋值然后退出
			lk.version = ver
			return
		}

		// 获取成功，尝试获取分布式锁
		if e == rpc.ErrNoKey || (rpc.Ok(e) && value == Empty) {
			if rpc.Ok(lk.ck.Put(lk.state, lk.secret, ver)) { // 如果拿锁成功了也要再循环一次获取新 version
				continue
			}
			// 拿锁失败直接循环等
			continue
		}
	}
}

// Release 将全力尝试释放锁，
func (lk *Lock) Release() {
	// 说明 lk 还没有 acquire，那不需要释放
	if lk.version == rpc.ZERO {
		return
	}

	v, _, _ := lk.ck.Get(lk.state)
	if v != lk.secret { // 说明不是自己的锁，那就不用释放了
		return
	}

	lk.release()
}

// put 一个空上去, 告诉其他 lock 这里已经释放了
func (lk *Lock) release() rpc.Err {
	return lk.ck.Put(lk.state, Empty, lk.version)
}
