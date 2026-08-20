package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	lockname string
	cliendID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, lockname: lockname, cliendID: kvtest.RandValue(12)} // 有ID重复的风险啊
	return lk
}

func (lk *Lock) Acquire() {
	for {
		lockname := lk.lockname
		value, version, err := lk.ck.Get(lockname)
		if err == rpc.ErrNoKey {
			version = 0
		}

		if value == "" {
			lk.ck.Put(lockname, lk.cliendID, version) // 可能失败, 那是task3的事情
		}
		

		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	
}
