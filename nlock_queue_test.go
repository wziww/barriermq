package barriermq

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/wziww/barriermq/internal"
)

func newNlockQueue(_size int64) *NLockQueue {
	nq := NLockQueue{
		wakeup:  make(chan int, 1),
		handler: _defaultHandler,
	}
	size := internal.GetSize(uint64(_size))
	if size > math.MaxInt64 {
		panic(ErrOutOfRange)
	}
	nq._mask = size - 1
	nq.size = int64(size)
	if _size > 0 {
		nq.queue = make([]q, size)
	}
	return &nq
}
func TestPush(t *testing.T) {
	var _size int64 = 10
	q := newNlockQueue(_size)
	__size := internal.GetSize(10)
	for i := 0; i < int(__size); i++ {
		if !q.Push(nil) {
			t.Fatal("non block queue push error -- should success")
		}
	}
	if q.Push(nil) {
		t.Fatal("non block queue push error -- should fail")
	}
	if q.head != 16 {
		t.Fatal("head position error")
	}
	if q.tail != 0 {
		t.Fatal("tail position error")
	}
	q.background(false)
	if q.head != 16 {
		t.Fatal("head position error")
	}
	if q.head != 16 {
		t.Fatal("tail position error")
	}
	for i := 0; i < int(__size); i++ {
		if !q.Push(nil) {
			t.Fatal("non block queue push error -- should success")
		}
	}
}
func TestMultiplePush(t *testing.T) {
	var _size int64 = 10
	q := newNlockQueue(_size)
	var g sync.WaitGroup
	var err uint32
	__size := internal.GetSize(10)
	for i := 0; i < int(__size)+10; i++ {
		g.Add(1)
		go func() {
			defer g.Done()
			if !q.Push(nil) {
				atomic.AddUint32(&err, 1)
			}
		}()
	}
	g.Wait()
	if atomic.LoadUint32(&err) != 10 {
		t.Fatal("non block queue push error -- success / failed should be 16 / 10")
	}
	if q.Push(nil) {
		t.Fatal("non block queue push error -- should fail")
	}
	if q.head != 16 {
		t.Fatal("head position error")
	}
	if q.tail != 0 {
		t.Fatal("tail position error")
	}
}
