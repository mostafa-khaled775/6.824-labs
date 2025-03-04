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
	ck      kvtest.IKVClerk
	key     string
	value   string
	version rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, key: l, value: kvtest.RandValue(8)}
	return lk
}

func (lk *Lock) Acquire() {
	value, version, err := lk.ck.Get(lk.key)
	if err == rpc.ErrNoKey || value == "released" {
		if err == rpc.ErrNoKey {
			version = 0
		}
		if putErr := lk.ck.Put(lk.key, lk.value, version); putErr != rpc.OK {
			time.Sleep(time.Millisecond * 10)
			lk.Acquire()
			return
		}
		lk.version = version + 1
		return
	} else if value == lk.value {
		lk.version = version
		return
	} else {
		time.Sleep(time.Millisecond * 10)
		lk.Acquire()
	}
}

func (lk *Lock) Release() {
	// we do not to check for the error here since
	// we are the only one who can release the lock
	lk.ck.Put(lk.key, "released", lk.version)
}
