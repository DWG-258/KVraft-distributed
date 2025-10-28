package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck          kvtest.IKVClerk
	lockversion rpc.Tversion
	lockname    string
	LockID      string
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.lockname = l
	return lk
}

const (
	Locked   = "locked"
	Unlocked = "unlocked"
)

func (lk *Lock) Acquire() {
	// Your code here
	lk.LockID = kvtest.RandValue(8)

	for {
		lockstate, lockversion, err := lk.ck.Get(lk.lockname)
		if err == rpc.ErrNoKey && lockversion == 0 {
			//first get lock ,no lock ,set lock
			ok := lk.ck.Put(lk.lockname, lk.LockID+Locked, 0)
			if ok == rpc.OK {
				lk.lockversion = lockversion + 1
				return
			}
		} else if err == rpc.OK {
			if lockstate == Unlocked {
				//acquire success
				ok := lk.ck.Put(lk.lockname, lk.LockID+Locked, lockversion)
				if ok == rpc.OK || ok == rpc.ErrMaybe {
					lk.lockversion = lockversion + 1
					return
				}

			}

		}

	}

}

func (lk *Lock) Release() {
	// Your code here
	for {
		err := lk.ck.Put(lk.lockname, Unlocked, lk.lockversion)
		switch err {
		case rpc.OK:
			return
		case rpc.ErrMaybe:
			return
		}
		print("lock")

		time.Sleep(1000 * time.Millisecond)

	}

}
