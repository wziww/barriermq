package barriermq

import (
	"errors"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/wziww/barriermq/internal"
)

var _size int64 = 1000

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
	q := newNlockQueue(_size)
	__size := internal.GetSize(uint64(_size))
	for i := 0; i < int(__size); i++ {
		if !q.Push(nil) {
			t.Fatal("non block queue push error -- should success")
		}
	}
	if q.Len() != int64(__size) {
		t.Fatalf("nlock_queue Len func error")
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
	if q.Len() != 0 {
		t.Fatalf("nlock_queue Len func error")
	}
	if q.Cap() != int64(__size) {
		t.Fatalf("nlock_queue Cap func error")
	}
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
	q.RegistHandler(func(data interface{}) error {
		return errors.New("test")
	})
	if q.Cap() != 0 {
		t.Fatalf("nlock_queue Cap func error")
	}
	if q.Len() != int64(__size) {
		t.Fatalf("nlock_queue Len func error")
	}
}
func TestMultiplePush(t *testing.T) {
	q := newNlockQueue(_size)
	var g sync.WaitGroup
	var err uint32
	__size := internal.GetSize(uint64(_size))
	for i := 0; i < int(__size)+100; i++ {
		g.Add(1)
		go func() {
			defer g.Done()
			if !q.Push(nil) {
				atomic.AddUint32(&err, 1)
			}
		}()
	}
	g.Wait()
	if atomic.LoadUint32(&err) != 100 {
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

func BenchmarkMultipleProducerNConsumer(b *testing.B) {
	var testnums int = 10000 * b.N
	q := newNlockQueue(_size)
	var wg sync.WaitGroup
	q.RegistHandler(func(_ interface{}) error {
		go wg.Done()
		return nil
	})
	wg.Add(testnums)
	b.ResetTimer()
	// consumer
	go q.background(true)
	// producer
	go b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for i := 0; i < testnums/b.N; i++ {
				for !q.Push(nil) {
					runtime.Gosched()
				}
			}
		}
	})
	wg.Wait()
}

func BenchmarkMultipleProducerNConsumerChan(b *testing.B) {
	__size := internal.GetSize(uint64(_size))
	var testnums int = 10000 * b.N
	var wg sync.WaitGroup
	wg.Add(testnums)
	ch := make(chan int, __size)
	b.ResetTimer()
	// consumer
	go func() {
		for {
			select {
			case <-ch:
				go wg.Done()
			}
		}
	}()
	// producer
	go b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for i := 0; i < testnums/b.N; i++ {
				ch <- 1
			}
		}
	})
	wg.Wait()
}

func BenchmarkProducerNConsumer(b *testing.B) {
	var testnums int = 100000
	q := newNlockQueue(_size)
	_ = internal.GetSize(uint64(_size))
	var wg sync.WaitGroup
	q.RegistHandler(func(_ interface{}) error {
		go wg.Done()
		return nil
	})
	wg.Add(testnums)
	b.ResetTimer()
	// producer
	go func() {
		for i := 0; i < testnums; i++ {
			for !q.Push(nil) {
				runtime.Gosched()
			}
		}
	}()
	// consumer
	go q.background(true)
	wg.Wait()
}

func BenchmarkProducerNConsumerChan(b *testing.B) {
	__size := internal.GetSize(uint64(_size))
	var testnums int = 100000
	var wg sync.WaitGroup
	wg.Add(testnums)
	ch := make(chan int, __size)
	b.ResetTimer()
	// producer
	go func() {
		for i := 0; i < testnums; i++ {
			ch <- 1
		}
	}()
	// consumer
	go func() {
		for {
			select {
			case <-ch:
				go wg.Done()
			}
		}
	}()
	wg.Wait()
}
