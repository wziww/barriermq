package barriermq

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wziww/barriermq/internal"
)

const (
	SPIN_TRIES = 10
)

const (
	// FreeStatus ...
	FreeStatus = iota
	// LockStatus ...
	LockStatus = iota
)

var (
	// ErrOutOfRange ...
	ErrOutOfRange error = errors.New("config memqueue size out of range")
)

type q struct {
	data interface{}
}

// NLockQueue ...
// No Locker Qeueue
type NLockQueue struct {
	_lock        sync.RWMutex
	head, tail   uint64
	queue        []q
	size         int64
	_mask        uint64
	wakeup       chan int
	done         int32
	handler      Handler
	Success      uint64
	Failed       uint64
	FullWaitTime time.Duration
}

var _defaultHandler Handler = func(data interface{}) error {
	return nil
}

// Len ...
func (q *NLockQueue) Len() int64 {
	return int64(atomic.LoadUint64(&q.head) - atomic.LoadUint64(&q.tail))
}

// Cap ...
func (q *NLockQueue) Cap() int64 {
	return q.size - q.Len()
}

// NewNlockQueue ...
func NewNlockQueue(_size int64) *NLockQueue {
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
	go nq.background(true)
	return &nq
}

// RegistHandler ...
func (q *NLockQueue) RegistHandler(fn Handler) {
	q._lock.Lock()
	defer q._lock.Unlock()
	q.handler = fn

}

// Push put message
func (q *NLockQueue) Push(msg interface{}) bool {
	var current uint64 = atomic.LoadUint64(&q.head)
	next := current + 1
	for {
		if q.Cap() > 0 { // cap > 0
		} else {
			atomic.AddUint64(&q.Failed, 1)
			return false
		}
		if atomic.CompareAndSwapUint64(&q.head, current, next) {
			q.queue[current&q._mask].data = msg
			if atomic.CompareAndSwapInt32(&q.done, FreeStatus, LockStatus) {
				q.wakeup <- 1
			}
			atomic.AddUint64(&q.Success, 1)
			return true
		}
		current = atomic.LoadUint64(&q.head)
		next = current + 1
	}
}

// @lp if loop ? [true/false]
func (q *NLockQueue) background(lp bool) {
	var count int
loop:
	count = SPIN_TRIES
	for ; count > 0; count-- {
		nums := q.Len()
		if nums <= 0 {
			continue
		}
		for ; nums > 0; nums-- {
			q._lock.RLock()
			if err := q.handler(q.queue[atomic.LoadUint64(&q.tail)&q._mask].data); err == nil {
				q._lock.RUnlock()
				q.queue[atomic.LoadUint64(&q.tail)&q._mask].data = nil // avoid stuck affect GC
				atomic.AddUint64(&q.tail, 1)
			} else { // error ? stuck then
				q._lock.RUnlock()
				goto next
			}
		}
		goto next
	}
next:
	if lp {
		timer := time.NewTimer(time.Millisecond * q.FullWaitTime)
		select {
		case <-timer.C:
			goto loop
		case <-q.Wakeup():
			timer.Stop()
			q.Sleep()
			goto loop
		}
	} else {
		return
	}
}

// Wakeup ...
func (q *NLockQueue) Wakeup() <-chan int {
	return q.wakeup
}

// Sleep ...
func (q *NLockQueue) Sleep() {
	atomic.StoreInt32(&q.done, FreeStatus)
}
