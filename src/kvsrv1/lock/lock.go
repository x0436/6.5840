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
		ownerID, version, err := lk.ck.Get(lk.lockname)
		if err == rpc.ErrNoKey {
			ownerID = ""
			version = 0
		}

		if ownerID == lk.cliendID {
			return
		}

		if ownerID == "" {
			err = lk.ck.Put(lk.lockname, lk.cliendID, version)
			if err == rpc.OK {
				return
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	for {
		ownerID, version, err := lk.ck.Get(lk.lockname)
		if err == rpc.ErrNoKey {
			return
		}

		if ownerID == "" {
			return
		}

		if ownerID == lk.cliendID {
			err = lk.ck.Put(lk.lockname, "", version)
			if err == rpc.OK {
				return
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}
